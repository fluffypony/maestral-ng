from maestral.utils.hashing import DropboxContentHasher, sha256_content_hasher


def test_sha256_content_hasher() -> None:
    hasher = sha256_content_hasher()
    hasher.update(b"Maestral")
    assert (
        hasher.hexdigest()
        == "a202ca9ba716078f88a700d19ddda6ad09875084d79244c5531d5392b303e606"
    )


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
