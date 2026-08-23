# Maestral Cryptomator sidecar

This process gives Maestral a narrow interface to the official Cryptomator
CryptoFS implementation. It reads newline-delimited JSON from standard input
and writes one response for each request to standard output. A vault password
may enter only through this private standard-input channel. The process never
accepts a password in an argument, environment variable, or file.

The sidecar uses CryptoFS 2.10.0 and Cryptolib 2.2.2. It creates and opens vault
format 8. Maestral keeps the unlock password in the system keyring. The remote
vault contains only the password-protected master key and encrypted data.

The `snapshot` and `storage_map` methods return pages with `entries` and
`next_cursor` fields. Start without a cursor, then pass each returned cursor to
read the next page. The optional `limit` field can reduce the default page size
of 4,096 entries. Each cursor keeps one stable view while the vault changes.
The process permits eight active views and limits each page's entry data to
8 MiB.

Each `storage_map` entry has a logical `path`, a `type`, and a `storage_path`.
The storage path uses `/` separators and is relative to the vault root.
CryptoFS supplies the physical path. A directory points to its stable `d/...`
content directory. A shortened file points to its `.c9s/contents.c9r` object.
Results use logical-path order. A missing or invalid physical object returns
`storage_mapping_missing`.

Build and test with JDK 25 or later:

```sh
mvn verify
java -jar target/maestral-cryptomator-sidecar-all.jar --exchange-root /private/path
```

The sidecar code is AGPL-3.0-or-later because it links to CryptoFS. It remains
an isolated process so the MIT-licensed Python daemon does not load CryptoFS or
its dependencies.
