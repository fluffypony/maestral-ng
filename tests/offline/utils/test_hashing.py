from maestral.utils.hashing import DropboxContentHasher


def test_content_hasher_copy_can_be_used_independently() -> None:
    hasher = DropboxContentHasher()
    hasher.update(b"shared-prefix")

    clone = hasher.copy()
    hasher.update(b"-original")
    clone.update(b"-clone")

    expected_original = DropboxContentHasher()
    expected_original.update(b"shared-prefix-original")
    expected_clone = DropboxContentHasher()
    expected_clone.update(b"shared-prefix-clone")

    assert hasher.digest_size == clone.digest_size
    assert hasher.hexdigest() == expected_original.hexdigest()
    assert clone.hexdigest() == expected_clone.hexdigest()
