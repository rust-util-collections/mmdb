use mmdb::{basic_multi_key::mapx_double_key::MapxDk, mmdb_set_base_dir};
use ruc::*;

#[test]
fn basic_cases() {
    info_omit!(mmdb_set_base_dir(&format!(
        "/tmp/mmdb_testing/{}",
        rand::random::<u64>()
    )));

    let mut map = MapxDk::new();

    assert!(map.insert(&(&1u8, &1u8), &9u8).is_none());
    assert!(map.insert(&(&1, &2), &8).is_none());
    assert!(map.insert(&(&1, &3), &7).is_none());

    assert_eq!(map.get(&(&1, &1)).unwrap(), 9);
    assert_eq!(map.get(&(&1, &2)).unwrap(), 8);
    assert_eq!(map.get(&(&1, &3)).unwrap(), 7);

    // does not exist
    assert!(map.remove(&(&1, Some(&4))).is_none());

    assert!(map.remove(&(&1, Some(&1))).is_some());
    assert!(map.get(&(&1, &1)).is_none());

    // partial-path remove
    assert!(map.remove(&(&1, None)).is_none()); // yes, is none
    assert!(map.get(&(&1, &2)).is_none());
    assert!(map.get(&(&1, &3)).is_none());

    map.entry(&(&1, &99)).or_insert(100);
    assert_eq!(map.get(&(&1, &99)).unwrap(), 100);
}
