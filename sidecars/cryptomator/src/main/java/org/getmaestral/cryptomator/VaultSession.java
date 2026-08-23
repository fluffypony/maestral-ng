/*
 * Copyright (C) 2026 The Maestral contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.getmaestral.cryptomator;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.cryptomator.cryptofs.CryptoFileSystem;
import org.cryptomator.cryptofs.CryptoFileSystemProperties;
import org.cryptomator.cryptofs.CryptoFileSystemProvider;
import org.cryptomator.cryptofs.VaultConfig;
import org.cryptomator.cryptolib.api.Masterkey;
import org.cryptomator.cryptolib.api.MasterkeyLoader;
import org.cryptomator.cryptolib.common.MasterkeyFileAccess;

import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

final class VaultSession implements AutoCloseable {
    static final int MAX_INLINE_BYTES = 1024 * 1024;
    static final int MAX_SNAPSHOT_ENTRIES = 1_000_000;
    static final String MASTERKEY_FILE = "masterkey.cryptomator";
    static final String CONFIG_FILE = "vault.cryptomator";

    private static final java.net.URI MASTERKEY_URI =
            java.net.URI.create("masterkeyfile:masterkey.cryptomator");
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Path exchangeRoot;
    private final Path vaultPath;
    private final CryptoFileSystem fileSystem;
    private final Path cleartextRoot;

    private VaultSession(Path exchangeRoot, Path vaultPath, CryptoFileSystem fileSystem) {
        this.exchangeRoot = exchangeRoot;
        this.vaultPath = vaultPath;
        this.fileSystem = fileSystem;
        this.cleartextRoot = fileSystem.getPath("/");
    }

    static VaultSession initialize(Path exchangeRoot, Path requestedPath, char[] passphrase)
            throws IOException {
        Path target = normalizeNewVaultPath(requestedPath);
        Path parent = target.getParent();
        if (parent == null) {
            throw new SidecarException("invalid_vault_path", "The vault path has no parent.");
        }
        Files.createDirectories(parent);
        Path realParent = parent.toRealPath();
        target = realParent.resolve(target.getFileName());
        requireSeparateRoots(exchangeRoot, target);

        boolean targetExists = Files.exists(target, LinkOption.NOFOLLOW_LINKS);
        if (targetExists) {
            if (Files.isSymbolicLink(target)
                    || !Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)
                    || !isEmptyDirectory(target)) {
                throw new SidecarException(
                        "vault_not_empty", "A new vault needs a missing or empty directory.");
            }
        }

        Path temporary = realParent.resolve(".maestral-vault-" + UUID.randomUUID() + ".tmp");
        Files.createDirectory(temporary);
        PathSecurity.setPrivateDirectoryPermissions(temporary);
        boolean installed = false;
        try {
            initializeVaultFiles(temporary, passphrase);
            try (CryptoFileSystem validation = openFileSystem(temporary, passphrase)) {
                // Opening the new vault verifies its signed config and protected master key.
                if (!validation.isOpen()) {
                    throw new IOException("The new vault did not open.");
                }
            }
            if (targetExists) {
                Files.delete(target);
            }
            moveAtomically(temporary, target, false);
            installed = true;
            return open(exchangeRoot, target, passphrase);
        } finally {
            if (!installed) {
                deleteTreeIfPresent(temporary);
            }
        }
    }

    static VaultSession open(Path exchangeRoot, Path requestedPath, char[] passphrase)
            throws IOException {
        Path vault = requestedPath.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(vault) || !Files.isDirectory(vault, LinkOption.NOFOLLOW_LINKS)) {
            throw new SidecarException("invalid_vault", "The vault is not a directory.");
        }
        vault = vault.toRealPath(LinkOption.NOFOLLOW_LINKS);
        requireSeparateRoots(exchangeRoot, vault);
        CryptoFileSystem fs = openFileSystem(vault, passphrase);
        return new VaultSession(exchangeRoot, vault, fs);
    }

    JsonObject info() throws IOException {
        VaultConfig.UnverifiedVaultConfig config =
                VaultConfig.decode(
                        Files.readString(
                                vaultPath.resolve(CONFIG_FILE), StandardCharsets.US_ASCII));
        JsonObject result = new JsonObject();
        result.addProperty("vault_path", vaultPath.toString());
        result.addProperty("vault_format", config.allegedVaultVersion());
        result.addProperty("shortening_threshold", config.allegedShorteningThreshold());
        result.addProperty("key_id", config.getKeyId().toString());
        return result;
    }

    JsonObject stat(String logicalPath, boolean includeHash) throws IOException {
        Path path = resolveLogical(logicalPath);
        return metadata(path, includeHash);
    }

    JsonArray list(String logicalPath, boolean includeHash) throws IOException {
        Path directory = resolveLogical(logicalPath);
        List<Path> children = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            stream.forEach(children::add);
        }
        children.sort(Comparator.comparing(path -> path.getFileName().toString()));

        JsonArray result = new JsonArray();
        for (Path child : children) {
            result.add(metadata(child, includeHash));
        }
        return result;
    }

    JsonArray snapshot(boolean includeHash) throws IOException {
        List<Path> paths;
        try (var stream = Files.walk(cleartextRoot)) {
            paths =
                    stream.filter(path -> !path.equals(cleartextRoot))
                            .limit(MAX_SNAPSHOT_ENTRIES + 1L)
                            .toList();
        }
        if (paths.size() > MAX_SNAPSHOT_ENTRIES) {
            throw new SidecarException(
                    "vault_too_large", "The vault snapshot has too many entries.");
        }
        paths = new ArrayList<>(paths);
        paths.sort(Comparator.comparing(this::toLogicalPath));

        JsonArray result = new JsonArray();
        for (Path path : paths) {
            result.add(metadata(path, includeHash));
        }
        return result;
    }

    JsonArray storageMap() throws IOException {
        List<StorageEntry> entries = storageEntries();
        Path storageRoot = vaultPath.resolve("d").normalize();
        JsonArray result = new JsonArray();
        for (StorageEntry entry : entries) {
            Path storagePath = entry.storagePath();
            BasicFileAttributes attrs;
            try {
                attrs =
                        Files.readAttributes(
                                storagePath,
                                BasicFileAttributes.class,
                                LinkOption.NOFOLLOW_LINKS);
            } catch (NoSuchFileException exc) {
                throw new SidecarException(
                        "storage_mapping_missing",
                        "A vault entry has no physical storage mapping.");
            }
            if (!entry.accepts(attrs)) {
                throw new SidecarException(
                        "storage_mapping_missing",
                        "A vault entry has no physical storage mapping.");
            }
            JsonObject item = new JsonObject();
            item.addProperty("path", entry.logicalPath());
            item.addProperty("type", entry.type());
            item.addProperty("storage_path", toStoragePath(storageRoot, storagePath));
            result.add(item);
        }
        return result;
    }

    void makeDirectory(String logicalPath, boolean parents) throws IOException {
        Path path = requireNonRoot(logicalPath);
        if (parents) {
            Files.createDirectories(path);
        } else {
            Files.createDirectory(path);
        }
    }

    void putFile(String logicalPath, String exchangePath, Long modifiedMillis, boolean replace)
            throws IOException {
        Path source = PathSecurity.resolveExchangeInput(exchangeRoot, exchangePath);
        putBytesOrFile(requireNonRoot(logicalPath), source, null, modifiedMillis, replace);
    }

    void writeInline(String logicalPath, String encoded, Long modifiedMillis, boolean replace)
            throws IOException {
        byte[] content;
        try {
            content = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException exc) {
            throw new SidecarException(
                    "invalid_request", "The inline content is not valid base64.");
        }
        if (content.length > MAX_INLINE_BYTES) {
            Arrays.fill(content, (byte) 0);
            throw new SidecarException(
                    "content_too_large", "Use an exchange file for large content.");
        }
        try {
            putBytesOrFile(requireNonRoot(logicalPath), null, content, modifiedMillis, replace);
        } finally {
            Arrays.fill(content, (byte) 0);
        }
    }

    void getFile(String logicalPath, String exchangePath, boolean replace) throws IOException {
        Path source = resolveLogical(logicalPath);
        Path target = PathSecurity.resolveExchangeOutput(exchangeRoot, exchangePath);
        if (!replace && Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(target.toString());
        }

        Path temporary =
                target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
            Files.copy(source, temporary);
            moveAtomically(temporary, target, replace);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    String readInline(String logicalPath) throws IOException {
        Path path = resolveLogical(logicalPath);
        long size = Files.size(path);
        if (size > MAX_INLINE_BYTES) {
            throw new SidecarException(
                    "content_too_large", "Use an exchange file for large content.");
        }
        return Base64.getEncoder().encodeToString(Files.readAllBytes(path));
    }

    void move(String sourcePath, String targetPath, boolean replace) throws IOException {
        Path source = requireNonRoot(sourcePath);
        Path target = requireNonRoot(targetPath);
        moveAtomically(source, target, replace);
    }

    void delete(String logicalPath, boolean recursive) throws IOException {
        Path path = requireNonRoot(logicalPath);
        if (!recursive || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            Files.delete(path);
            return;
        }
        Files.walkFileTree(
                path,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path directory, IOException exc)
                            throws IOException {
                        if (exc != null) {
                            throw exc;
                        }
                        Files.delete(directory);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    void createLink(String logicalPath, String target) throws IOException {
        if (target.indexOf('\0') >= 0) {
            throw new SidecarException("invalid_path", "The link target is invalid.");
        }
        Files.createSymbolicLink(requireNonRoot(logicalPath), fileSystem.getPath(target));
    }

    String readLink(String logicalPath) throws IOException {
        return Files.readSymbolicLink(resolveLogical(logicalPath)).toString();
    }

    @Override
    public void close() throws IOException {
        fileSystem.close();
    }

    private static void initializeVaultFiles(Path vault, char[] passphrase) throws IOException {
        MasterkeyFileAccess keyAccess = new MasterkeyFileAccess(new byte[0], RANDOM);
        try (Masterkey masterkey = Masterkey.generate(RANDOM)) {
            keyAccess.persist(
                    masterkey, vault.resolve(MASTERKEY_FILE), CharBuffer.wrap(passphrase));
        }
        CryptoFileSystemProvider.initialize(vault, properties(vault, passphrase), MASTERKEY_URI);
    }

    private static CryptoFileSystem openFileSystem(Path vault, char[] passphrase)
            throws IOException {
        return CryptoFileSystemProvider.newFileSystem(vault, properties(vault, passphrase));
    }

    private static CryptoFileSystemProperties properties(Path vault, char[] passphrase) {
        MasterkeyLoader loader =
                keyId -> {
                    if (!MASTERKEY_URI.equals(keyId)) {
                        throw new SidecarException(
                                "invalid_key_id", "The vault uses an unsupported key ID.");
                    }
                    return new MasterkeyFileAccess(new byte[0], RANDOM)
                            .load(vault.resolve(MASTERKEY_FILE), CharBuffer.wrap(passphrase));
                };
        return CryptoFileSystemProperties.cryptoFileSystemProperties()
                .withKeyLoader(loader)
                .build();
    }

    private static Path normalizeNewVaultPath(Path requestedPath) {
        if (!requestedPath.isAbsolute()) {
            throw new SidecarException("invalid_vault_path", "The vault path must be absolute.");
        }
        Path target = requestedPath.normalize();
        if (target.getFileName() == null || Files.isSymbolicLink(target)) {
            throw new SidecarException("invalid_vault_path", "The vault path is invalid.");
        }
        return target;
    }

    private static boolean isEmptyDirectory(Path path) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            return !stream.iterator().hasNext();
        }
    }

    private static void requireSeparateRoots(Path exchangeRoot, Path vault) {
        Path normalizedVault = vault.toAbsolutePath().normalize();
        if (normalizedVault.startsWith(exchangeRoot) || exchangeRoot.startsWith(normalizedVault)) {
            throw new SidecarException(
                    "overlapping_roots", "The vault and exchange roots must be separate.");
        }
    }

    private static void moveAtomically(Path source, Path target, boolean replace)
            throws IOException {
        List<CopyOption> options = new ArrayList<>();
        options.add(StandardCopyOption.ATOMIC_MOVE);
        if (replace) {
            options.add(StandardCopyOption.REPLACE_EXISTING);
        }
        try {
            Files.move(source, target, options.toArray(CopyOption[]::new));
        } catch (AtomicMoveNotSupportedException ignored) {
            if (replace) {
                Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.move(source, target);
            }
        }
    }

    private static void deleteTreeIfPresent(Path root) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        Files.walkFileTree(
                root,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path directory, IOException exc)
                            throws IOException {
                        if (exc != null) {
                            throw exc;
                        }
                        Files.delete(directory);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    private void putBytesOrFile(
            Path target, Path source, byte[] content, Long modifiedMillis, boolean replace)
            throws IOException {
        if (!replace && Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(target.toString());
        }
        Path parent = target.getParent();
        if (parent == null || !Files.isDirectory(parent)) {
            throw new NoSuchFileException("The destination parent does not exist.");
        }

        Path temporary = target.resolveSibling(".maestral-upload-" + UUID.randomUUID() + ".tmp");
        try {
            if (source != null) {
                Files.copy(source, temporary);
            } else if (content != null) {
                Files.write(temporary, content);
            } else {
                throw new IllegalStateException("No upload content");
            }
            if (modifiedMillis != null) {
                Files.setLastModifiedTime(temporary, FileTime.fromMillis(modifiedMillis));
            }
            moveAtomically(temporary, target, replace);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private JsonObject metadata(Path path, boolean includeHash) throws IOException {
        BasicFileAttributes attrs =
                Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        JsonObject result = new JsonObject();
        result.addProperty("path", toLogicalPath(path));
        if (attrs.isSymbolicLink()) {
            result.addProperty("type", "symlink");
            String target = Files.readSymbolicLink(path).toString();
            result.addProperty("link_target", target);
            result.addProperty("size", target.getBytes(StandardCharsets.UTF_8).length);
        } else if (attrs.isDirectory()) {
            result.addProperty("type", "directory");
            result.addProperty("size", 0);
        } else if (attrs.isRegularFile()) {
            result.addProperty("type", "file");
            result.addProperty("size", attrs.size());
            if (includeHash) {
                result.addProperty("sha256", sha256(path));
            }
        } else {
            result.addProperty("type", "other");
            result.addProperty("size", attrs.size());
        }
        result.addProperty("modified_ms", attrs.lastModifiedTime().toMillis());
        return result;
    }

    private List<StorageEntry> storageEntries() throws IOException {
        List<Path> paths;
        try (var stream = Files.walk(cleartextRoot)) {
            paths =
                    stream.filter(path -> !path.equals(cleartextRoot))
                            .limit(MAX_SNAPSHOT_ENTRIES + 1L)
                            .toList();
        }
        if (paths.size() > MAX_SNAPSHOT_ENTRIES) {
            throw new SidecarException(
                    "vault_too_large", "The vault storage map has too many entries.");
        }
        paths = new ArrayList<>(paths);
        paths.sort(Comparator.comparing(this::toLogicalPath));

        List<StorageEntry> entries = new ArrayList<>(paths.size());
        for (Path path : paths) {
            BasicFileAttributes attrs =
                    Files.readAttributes(
                            path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            entries.add(
                    new StorageEntry(
                            toLogicalPath(path),
                            storageType(attrs),
                            fileSystem.getCiphertextPath(path)));
        }
        return entries;
    }

    private static String storageType(BasicFileAttributes attrs) {
        if (attrs.isSymbolicLink()) {
            return "symlink";
        } else if (attrs.isDirectory()) {
            return "directory";
        } else if (attrs.isRegularFile()) {
            return "file";
        }
        throw new SidecarException(
                "storage_mapping_missing", "A vault entry has no physical storage mapping.");
    }

    private String toStoragePath(Path storageRoot, Path storagePath) {
        Path normalized = storagePath.toAbsolutePath().normalize();
        if (!normalized.startsWith(storageRoot) || normalized.equals(storageRoot)) {
            throw new SidecarException(
                    "storage_mapping_missing", "A vault entry has no physical storage mapping.");
        }

        Path relative = vaultPath.relativize(normalized).normalize();
        StringBuilder result = new StringBuilder();
        for (Path component : relative) {
            String value = component.toString();
            if (value.isEmpty()
                    || value.equals(".")
                    || value.equals("..")
                    || value.indexOf('/') >= 0
                    || value.indexOf('\\') >= 0) {
                throw new SidecarException(
                        "storage_mapping_missing",
                        "A vault entry has no physical storage mapping.");
            }
            if (!result.isEmpty()) {
                result.append('/');
            }
            result.append(value);
        }
        if (result.isEmpty()) {
            throw new SidecarException(
                    "storage_mapping_missing", "A vault entry has no physical storage mapping.");
        }
        return result.toString();
    }

    private String sha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exc) {
            throw new IllegalStateException("SHA-256 is unavailable", exc);
        }
        byte[] buffer = new byte[128 * 1024];
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        } finally {
            Arrays.fill(buffer, (byte) 0);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private Path requireNonRoot(String logicalPath) {
        Path path = resolveLogical(logicalPath);
        if (path.equals(cleartextRoot)) {
            throw new SidecarException(
                    "invalid_path", "The operation cannot change the vault root.");
        }
        return path;
    }

    private Path resolveLogical(String logicalPath) {
        if (!logicalPath.startsWith("/")
                || logicalPath.indexOf('\0') >= 0
                || logicalPath.indexOf('\\') >= 0
                || logicalPath.contains("//")) {
            throw new SidecarException("invalid_path", "The logical path is invalid.");
        }
        if (logicalPath.equals("/")) {
            return cleartextRoot;
        }

        Path resolved = cleartextRoot;
        for (String component : logicalPath.substring(1).split("/", -1)) {
            if (component.isEmpty() || component.equals(".") || component.equals("..")) {
                throw new SidecarException("invalid_path", "The logical path is invalid.");
            }
            resolved = resolved.resolve(component);
        }
        resolved = resolved.normalize();
        if (!resolved.startsWith(cleartextRoot)) {
            throw new SidecarException("invalid_path", "The logical path leaves the vault.");
        }
        return resolved;
    }

    private String toLogicalPath(Path path) {
        Path relative = cleartextRoot.relativize(path);
        StringBuilder result = new StringBuilder();
        for (Path component : relative) {
            result.append('/').append(component);
        }
        return result.isEmpty() ? "/" : result.toString();
    }

    private record StorageEntry(String logicalPath, String type, Path storagePath) {
        boolean accepts(BasicFileAttributes attrs) {
            return type.equals("directory") ? attrs.isDirectory() : attrs.isRegularFile();
        }
    }
}
