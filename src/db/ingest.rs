//! Bottommost sorted bulk ingestion (`DB::ingest_sorted`).

use super::*;

struct WriteLeaderGuard<'a>(&'a DB);

impl Drop for WriteLeaderGuard<'_> {
    fn drop(&mut self) {
        self.0.write_queue.lock().leader_active = false;
        self.0.write_cv.notify_all();
    }
}

fn version_overlaps_user_range(
    version: &crate::manifest::version::Version,
    min_key: &[u8],
    max_key: &[u8],
) -> bool {
    for level in 0..version.num_levels {
        for tf in version.level_files(level) {
            let s = types::user_key(&tf.meta.smallest_key);
            let l = types::user_key(&tf.meta.largest_key);
            if s <= max_key && l >= min_key {
                return true;
            }
            if tf.meta.has_range_deletions
                && tf
                    .reader
                    .range_partition_extents()
                    .any(|(begin, end)| begin <= max_key && end > min_key)
            {
                return true;
            }
        }
    }
    false
}

fn inner_overlaps_user_range(inner: &DBInner, min_key: &[u8], max_key: &[u8]) -> bool {
    if inner
        .active_memtable
        .overlaps_user_key_range(min_key, max_key)
    {
        return true;
    }
    if inner
        .immutable_memtables
        .iter()
        .any(|m| m.overlaps_user_key_range(min_key, max_key))
    {
        return true;
    }
    version_overlaps_user_range(&inner.versions.current(), min_key, max_key)
}

fn validate_ingest_entry(key: &[u8], val: &[u8]) -> Result<()> {
    if key.len() > types::MAX_USER_KEY_SIZE {
        return Err(Error::invalid_argument(format!(
            "key size exceeds maximum {}",
            types::MAX_USER_KEY_SIZE
        )));
    }
    if key.len().saturating_add(val.len()) > types::MAX_WRITE_ENTRY_SIZE {
        return Err(Error::invalid_argument(
            "ingest_sorted entry exceeds maximum write entry size",
        ));
    }
    Ok(())
}

impl DB {
    fn ingest_build_opts(&self, target_level: usize) -> TableBuildOptions {
        let compression = if !self.options.compression_per_level.is_empty()
            && target_level < self.options.compression_per_level.len()
        {
            self.options.compression_per_level[target_level]
        } else {
            self.options.compression
        };
        TableBuildOptions {
            block_size: self.options.block_size,
            block_restart_interval: self.options.block_restart_interval,
            bloom_bits_per_key: self.options.bloom_bits_for_level(target_level),
            prefix_bloom_bits_per_key: Some(self.options.bloom_bits_per_key),
            compression,
            prefix_len: self.options.prefix_len,
            block_property_collectors: self
                .options
                .block_property_collectors
                .iter()
                .map(|f| f())
                .collect(),
        }
    }

    /// Stream strictly ascending `(key, value)` pairs directly into bottommost
    /// (`num_levels - 1`) SST files without writing to the WAL or MemTable.
    ///
    /// Returns the number of ingested entries. An empty iterator is a no-op
    /// returning `Ok(0)`.
    ///
    /// # Preconditions & Errors
    ///
    /// - Keys must be strictly ascending (`prev_key < key`); duplicate or
    ///   out-of-order keys return [`ErrorKind::InvalidArgument`](crate::ErrorKind::InvalidArgument).
    /// - Keys and values must obey [`MAX_USER_KEY_SIZE`](crate::types::MAX_USER_KEY_SIZE)
    ///   and [`MAX_WRITE_ENTRY_SIZE`](crate::types::MAX_WRITE_ENTRY_SIZE)
    ///   (`InvalidArgument`).
    /// - The ingested `[min_key, max_key]` span must be strictly disjoint from
    ///   all point entries and range tombstones across active/immutable
    ///   memtables and every existing SST in `L0..num_levels - 1`
    ///   (`InvalidArgument`). On rejection or I/O error, newly written staging
    ///   SSTs are removed and no partial data becomes visible.
    /// - Active [`Snapshot`] handles taken before `ingest_sorted` completes do
    ///   not observe the ingested keys.
    pub fn ingest_sorted<I, K, V>(&self, iter: I) -> Result<u64>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.check_writable().ctx()?;
        let _maintenance = self.begin_maintenance()?;
        let _leader = {
            let mut wq = self.write_queue.lock();
            self.wait_for_write_leader_idle(&mut wq);
            self.check_writable().ctx()?;
            wq.leader_active = true;
            WriteLeaderGuard(self)
        };

        let mut iter = iter.into_iter();
        let Some(first_item) = iter.next() else {
            return Ok(0);
        };

        validate_ingest_entry(first_item.0.as_ref(), first_item.1.as_ref())?;

        let (ingest_seq, file_numbers) = {
            let inner = self.inner.lock();
            let first_k = first_item.0.as_ref();
            if inner_overlaps_user_range(&inner, first_k, first_k) {
                return Err(Error::invalid_argument(
                    "ingest_sorted key range overlaps existing database state",
                ));
            }
            let seq = self.sequence.load(Ordering::Acquire);
            if seq > MAX_SEQUENCE_NUMBER {
                return Err(Error::invalid_argument("sequence number space exhausted"));
            }
            (seq, inner.versions.file_numbers.clone())
        };

        let _build = self.builds.enter();
        let target_level = self.options.num_levels - 1;
        let target_file_size = self.options.target_file_size_base.max(1);
        let pending = PendingSsts::new(&self.storage_path, &file_numbers);
        let trailer = (!types::pack_sequence_and_type(ingest_seq, ValueType::Value)).to_be_bytes();

        let mut min_key: Option<Vec<u8>> = None;
        let mut prev_key: Option<Vec<u8>> = None;
        let mut ikey_buf = Vec::new();
        let mut current_builder: Option<(u64, TableBuilder, u64)> = None;
        let mut outputs: Vec<FileMetaData> = Vec::new();
        let mut count = 0u64;
        let mut total_user_bytes = 0u64;

        for (k_item, v_item) in std::iter::once(first_item).chain(iter) {
            let k = k_item.as_ref();
            let v = v_item.as_ref();
            validate_ingest_entry(k, v)?;
            if let Some(ref prev) = prev_key
                && k <= prev.as_slice()
            {
                return Err(Error::invalid_argument(
                    "ingest_sorted keys must be strictly ascending",
                ));
            }

            if let Some((_, ref b, cur_bytes)) = current_builder
                && (cur_bytes >= target_file_size
                    || b.projected_meta_size() >= META_BLOCK_SPLIT_THRESHOLD)
            {
                let (num, b, _) = current_builder.take().unwrap();
                outputs.push(flush::file_metadata((num, b.finish().ctx()?)));
            }

            if current_builder.is_none() {
                let num = pending.allocate().ctx()?;
                let sst_path = self.storage_path.join(format!("{num:06}.sst"));
                let b = TableBuilder::new(&sst_path, self.ingest_build_opts(target_level)).ctx()?;
                current_builder = Some((num, b, 0));
            }

            ikey_buf.clear();
            ikey_buf.extend_from_slice(k);
            ikey_buf.extend_from_slice(&trailer);

            let (_, b, cur_bytes) = current_builder.as_mut().unwrap();
            b.add(&ikey_buf, v).ctx()?;
            *cur_bytes = cur_bytes.saturating_add((ikey_buf.len() + v.len()) as u64);

            if min_key.is_none() {
                min_key = Some(k.to_vec());
            }
            let pk = prev_key.get_or_insert_with(Vec::new);
            pk.clear();
            pk.extend_from_slice(k);
            count += 1;
            total_user_bytes = total_user_bytes.saturating_add((k.len() + v.len()) as u64);
        }

        if let Some((num, b, _)) = current_builder.take() {
            outputs.push(flush::file_metadata((num, b.finish().ctx()?)));
        }

        let min_k = min_key.as_deref().expect("non-empty ingest");
        let max_k = prev_key.as_deref().expect("non-empty ingest");

        if inner_overlaps_user_range(&self.inner.lock(), min_k, max_k) {
            return Err(Error::invalid_argument(
                "ingest_sorted key range overlaps existing database state",
            ));
        }

        let synced = sync_new_sst_parent_dir(&self.dir_sync, &self.read.manifest_poisoned)?;
        self.table_cache.prewarm(outputs.iter().map(|m| m.number));

        let manifest_handle = {
            let mut inner = self.inner.lock();
            if let Err(e) = self.read.check_usable() {
                for m in &outputs {
                    self.table_cache.evict(m.number);
                }
                return Err(e);
            }
            if inner_overlaps_user_range(&inner, min_k, max_k) {
                for m in &outputs {
                    self.table_cache.evict(m.number);
                }
                return Err(Error::invalid_argument(
                    "ingest_sorted key range overlaps existing database state",
                ));
            }
            let mut edit = VersionEdit::new();
            edit.set_last_sequence(ingest_seq);
            edit.set_next_file_number(inner.versions.next_file_number());
            for meta in &outputs {
                edit.add_file(target_level as u32, meta.clone());
            }
            if let Err(e) = inner.versions.log_and_apply_synced(edit, synced) {
                for m in &outputs {
                    self.table_cache.evict(m.number);
                }
                return Err(e).ctx();
            }
            pending.keep();
            self.sequence.store(ingest_seq + 1, Ordering::Release);
            inner.versions.manifest_sync_handle()
        };

        if let Err(e) = confirm_manifest_durable(&manifest_handle, &self.read.manifest_poisoned) {
            self.set_bg_error(format!("ingest_sorted manifest sync failed: {e}"));
            return Err(e).ctx();
        }

        {
            let inner = self.inner.lock();
            self.read
                .committed_sequence
                .store(ingest_seq, Ordering::Release);
            self.read.stats.record_write(total_user_bytes);
            self.install_super_version(&inner);
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    #[test]
    fn ingest_sorted_splits_into_bottommost_level_with_zero_wal_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions::default()
            .with_num_levels(4)
            .with_target_file_size_base(4096);
        let db = DB::open(opts.clone(), dir.path()).unwrap();

        // Empty iterator is a no-op.
        let empty: Vec<(&[u8], &[u8])> = Vec::new();
        assert_eq!(db.ingest_sorted(empty).unwrap(), 0);
        assert_eq!(db.read.current_sequence(), 0);

        let rows: Vec<(Vec<u8>, Vec<u8>)> = (0..120u32)
            .map(|i| (format!("key_{i:05}").into_bytes(), vec![b'v'; 128]))
            .collect();
        let count = db
            .ingest_sorted(rows.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
            .unwrap();
        assert_eq!(count, 120);

        // Installed directly into L3 (`num_levels - 1`) across multiple SSTs,
        // with L0..L2 empty and zero WAL bytes written.
        {
            let inner = db.inner.lock();
            let v = inner.versions.current();
            for level in 0..3 {
                assert!(v.level_files(level).is_empty());
            }
            assert!(
                v.level_files(3).len() >= 3,
                "expected target_file_size_base splits in L3, got {}",
                v.level_files(3).len()
            );
            let wal_path = dir.path().join(format!("{:06}.wal", inner.wal_number));
            assert_eq!(
                fs::metadata(wal_path).unwrap().len(),
                0,
                "ingest_sorted must write 0 WAL bytes"
            );
        }

        db.close().unwrap();
        let reopened = DB::open_read_only(dir.path()).unwrap();
        let got: Vec<(Vec<u8>, Vec<u8>)> = reopened.iter().unwrap().collect();
        assert_eq!(got, rows);
    }

    #[test]
    fn ingest_sorted_preserves_snapshots_and_rejects_unsorted_or_overlapping_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open(DbOptions::default().with_num_levels(3), dir.path()).unwrap();

        db.put(b"b_mem", b"1").unwrap();
        db.delete_range(b"d_begin", b"d_end").unwrap();
        db.flush().unwrap();
        db.put(b"m_active", b"2").unwrap();
        db.delete_range(b"p_begin", b"p_end").unwrap();

        let snap_before = db.snapshot();

        // 1. Out-of-order and duplicate keys fail with InvalidArgument and leave no orphan SSTs.
        for bad in [
            vec![(b"z2".as_slice(), b"v".as_slice()), (b"z1", b"v")],
            vec![(b"z1".as_slice(), b"v1".as_slice()), (b"z1", b"v2")],
        ] {
            assert_eq!(
                db.ingest_sorted(bad).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
        }

        // 2. Overlaps against SST point keys, SST range tombstones, active memtable points,
        //    and active memtable range tombstones all fail with InvalidArgument.
        for overlapping in [
            vec![(b"b_mem".as_slice(), b"x".as_slice())],
            vec![(b"d_begin\x00".as_slice(), b"x".as_slice())],
            vec![(b"m_active".as_slice(), b"x".as_slice())],
            vec![(b"p_begin\x00".as_slice(), b"x".as_slice())],
            vec![(b"a_lo".as_slice(), b"x".as_slice()), (b"c_hi", b"y")],
        ] {
            assert_eq!(
                db.ingest_sorted(overlapping).unwrap_err().kind(),
                ErrorKind::InvalidArgument
            );
        }

        // 3. Disjoint spans (`a_*` before `b_mem`, and `z_*` after `p_end`) succeed,
        //    while `snap_before` remains isolated from both post-snapshot ingests.
        assert_eq!(
            db.ingest_sorted([(b"a_01", b"va1"), (b"a_02", b"va2")])
                .unwrap(),
            2
        );
        assert_eq!(
            db.ingest_sorted([(b"z_01", b"vz1"), (b"z_02", b"vz2")])
                .unwrap(),
            2
        );

        assert_eq!(snap_before.get(b"a_01").unwrap(), None);
        assert_eq!(snap_before.get(b"z_01").unwrap(), None);
        assert_eq!(
            snap_before.get(b"b_mem").unwrap().as_deref(),
            Some(b"1".as_slice())
        );

        assert_eq!(db.get(b"a_01").unwrap().as_deref(), Some(b"va1".as_slice()));
        assert_eq!(db.get(b"z_02").unwrap().as_deref(), Some(b"vz2".as_slice()));

        // Verify no orphan SSTs were left behind by the rejected ingests.
        let live_ssts: usize = {
            let v = db.inner.lock().versions.current();
            (0..v.num_levels).map(|l| v.level_files(l).len()).sum()
        };
        let disk_ssts = fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .is_some_and(|e| e.path().extension().is_some_and(|x| x == "sst"))
            })
            .count();
        assert_eq!(disk_ssts, live_ssts);
    }
}
