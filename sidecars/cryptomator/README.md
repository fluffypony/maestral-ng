# Maestral Cryptomator sidecar

This process gives Maestral a narrow interface to the official Cryptomator
CryptoFS implementation. It reads newline-delimited JSON from standard input
and writes one response for each request to standard output. A vault password
may enter only through this private standard-input channel. The process never
accepts a password in an argument, environment variable, or file.

The sidecar uses CryptoFS 2.10.0 and Cryptolib 2.2.2. It creates and opens vault
format 8. Maestral keeps the unlock password in the system keyring. The remote
vault contains only the password-protected master key and encrypted data.

Build and test with JDK 25 or later:

```sh
mvn verify
java -jar target/maestral-cryptomator-sidecar-all.jar --exchange-root /private/path
```

The sidecar code is AGPL-3.0-or-later because it links to CryptoFS. It remains
an isolated process so the MIT-licensed Python daemon does not load CryptoFS or
its dependencies.
