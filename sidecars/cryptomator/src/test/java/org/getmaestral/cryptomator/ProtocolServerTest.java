/*
 * Copyright (C) 2026 The Maestral contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.getmaestral.cryptomator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Comparator;

class ProtocolServerTest {
    private static final Gson GSON = new Gson();
    private static final String PASSWORD = "correct horse battery staple";

    @TempDir Path temporaryDirectory;

    private Path exchange;
    private Path vault;
    private ProtocolServer server;
    private long requestId;

    @BeforeEach
    void setUp() throws IOException {
        exchange = temporaryDirectory.resolve("exchange");
        Files.createDirectory(exchange);
        try {
            Files.setPosixFilePermissions(exchange, PosixFilePermissions.fromString("rwx------"));
        } catch (UnsupportedOperationException ignored) {
            // Windows uses the temporary directory ACL.
        }
        vault = temporaryDirectory.resolve("vault");
        server = new ProtocolServer(exchange.toRealPath());
    }

    @AfterEach
    void tearDown() throws IOException {
        server.close();
    }

    @Test
    void reportsPinnedOfficialImplementation() {
        JsonObject hello = call("hello", new JsonObject());

        assertEquals(1, hello.get("protocol_version").getAsInt());
        assertEquals("2.10.0", hello.get("cryptofs_version").getAsString());
        assertEquals("2.2.2", hello.get("cryptolib_version").getAsString());
        assertEquals(8, hello.get("vault_format").getAsInt());
        assertFalse(hello.get("vault_open").getAsBoolean());
    }

    @Test
    void returnsStructuredErrorsWithoutPaths() {
        JsonObject invalidJson = server.handle("not JSON");
        assertEquals("invalid_json", errorCode(invalidJson));

        JsonObject missing = callForResponse("stat", params("path", "/missing"));
        assertEquals("vault_not_open", errorCode(missing));
        assertFalse(missing.toString().contains(temporaryDirectory.toString()));
    }

    @Test
    void createsAndReopensOfficialVaultFormatEight() throws IOException {
        JsonObject info = initialize();
        assertEquals(8, info.get("vault_format").getAsInt());
        assertEquals(220, info.get("shortening_threshold").getAsInt());
        assertEquals("masterkeyfile:masterkey.cryptomator", info.get("key_id").getAsString());
        assertTrue(Files.isRegularFile(vault.resolve("vault.cryptomator")));
        assertTrue(Files.isRegularFile(vault.resolve("masterkey.cryptomator")));

        call("close", new JsonObject());
        JsonObject wrongSecret = params("vault_path", vault.toString());
        wrongSecret.addProperty("secret", encode("wrong password"));
        JsonObject rejected = callForResponse("open", wrongSecret);
        assertEquals("invalid_passphrase", errorCode(rejected));

        open();
        assertTrue(call("hello", new JsonObject()).get("vault_open").getAsBoolean());
    }

    @Test
    void handlesNamesContentDirectoriesMovesLinksAndShortening() throws Exception {
        initialize();
        call("mkdir", params("path", "/names"));
        call("mkdir", params("path", "/moves"));
        writeInline("/names/Grüße 東京.txt", "unicode content".getBytes(StandardCharsets.UTF_8));
        writeInline("/moves/source.bin", deterministicBytes(4097));

        JsonObject move = params("source", "/moves/source.bin");
        move.addProperty("target", "/moves/destination.bin");
        call("move", move);

        JsonObject link = params("path", "/names/relative-link");
        link.addProperty("target", "Grüße 東京.txt");
        call("symlink", link);
        assertEquals(
                "Grüße 東京.txt",
                call("readlink", params("path", "/names/relative-link"))
                        .get("target")
                        .getAsString());

        String longName = "l".repeat(190) + ".txt";
        writeInline("/names/" + longName, "long name".getBytes(StandardCharsets.UTF_8));

        JsonObject snapshotParams = new JsonObject();
        snapshotParams.addProperty("include_hash", true);
        JsonArray snapshot = callArray("snapshot", snapshotParams);
        assertTrue(hasPath(snapshot, "/moves/destination.bin"));
        assertFalse(hasPath(snapshot, "/moves/source.bin"));
        assertTrue(hasPath(snapshot, "/names/" + longName));
        assertTrue(
                snapshot.asList().stream()
                        .map(element -> element.getAsJsonObject())
                        .filter(
                                entry ->
                                        entry.get("path")
                                                .getAsString()
                                                .equals("/names/Grüße 東京.txt"))
                        .allMatch(entry -> entry.has("sha256")));

        byte[] read =
                Base64.getDecoder()
                        .decode(
                                call("read_inline", params("path", "/names/Grüße 東京.txt"))
                                        .get("content")
                                        .getAsString());
        assertArrayEquals("unicode content".getBytes(StandardCharsets.UTF_8), read);

        call("close", new JsonObject());
        try (var paths = Files.walk(vault)) {
            assertTrue(
                    paths.anyMatch(
                            path ->
                                    path.getFileName() != null
                                            && path.getFileName().toString().endsWith(".c9s")));
        }
    }

    @Test
    void mapsStablePhysicalStorageWithoutHostPaths() throws Exception {
        initialize();
        call("mkdir", params("path", "/stable-directory"));
        writeInline("/regular.txt", "regular".getBytes(StandardCharsets.UTF_8));
        writeInline("/stable-directory/child.txt", "child".getBytes(StandardCharsets.UTF_8));

        JsonObject link = params("path", "/relative-link");
        link.addProperty("target", "regular.txt");
        call("symlink", link);

        String longPath = "/" + "long-" + "x".repeat(185) + ".txt";
        writeInline(longPath, "long".getBytes(StandardCharsets.UTF_8));

        JsonArray beforeMove = callArray("storage_map", new JsonObject());
        assertEquals(5, beforeMove.size());
        assertSafeStoragePaths(beforeMove);
        assertFalse(beforeMove.toString().contains(temporaryDirectory.toString()));

        JsonObject regular = entryFor(beforeMove, "/regular.txt");
        assertEquals("file", regular.get("type").getAsString());
        assertTrue(regular.get("storage_path").getAsString().endsWith(".c9r"));
        assertFalse(regular.get("storage_path").getAsString().contains(".c9s/"));

        JsonObject directory = entryFor(beforeMove, "/stable-directory");
        assertEquals("directory", directory.get("type").getAsString());
        String directoryStoragePath = directory.get("storage_path").getAsString();
        assertTrue(
                Files.isDirectory(vault.resolve(directoryStoragePath), LinkOption.NOFOLLOW_LINKS));

        JsonObject symbolicLink = entryFor(beforeMove, "/relative-link");
        assertEquals("symlink", symbolicLink.get("type").getAsString());
        assertTrue(symbolicLink.get("storage_path").getAsString().endsWith(".c9r/symlink.c9r"));

        JsonObject shortened = entryFor(beforeMove, longPath);
        assertEquals("file", shortened.get("type").getAsString());
        assertTrue(shortened.get("storage_path").getAsString().contains(".c9s/contents.c9r"));

        JsonObject move = params("source", "/stable-directory");
        move.addProperty("target", "/moved-directory");
        call("move", move);
        JsonArray afterMove = callArray("storage_map", new JsonObject());
        assertEquals(
                directoryStoragePath,
                entryFor(afterMove, "/moved-directory").get("storage_path").getAsString());
        assertFalse(hasPath(afterMove, "/stable-directory"));
    }

    @Test
    void rejectsAnAmbiguousPhysicalStorageMappingWithoutPaths() throws Exception {
        initialize();
        writeInline("/target.txt", "content".getBytes(StandardCharsets.UTF_8));
        JsonObject target = entryFor(callArray("storage_map", new JsonObject()), "/target.txt");
        Path storagePath = vault.resolve(target.get("storage_path").getAsString());
        Files.createLink(vault.resolve("d/storage-map-duplicate"), storagePath);

        JsonObject response = callForResponse("storage_map", new JsonObject());
        assertEquals("storage_mapping_ambiguous", errorCode(response));
        assertFalse(response.toString().contains(temporaryDirectory.toString()));
    }

    @Test
    void transfersLargeFilesOnlyThroughPrivateExchange() throws Exception {
        initialize();
        byte[] content = deterministicBytes(VaultSession.MAX_INLINE_BYTES + 17);
        Path input = exchange.resolve("input.bin");
        Files.write(input, content);

        JsonObject put = params("path", "/large.bin");
        put.addProperty("exchange_path", "input.bin");
        call("put_file", put);

        JsonObject inline = callForResponse("read_inline", params("path", "/large.bin"));
        assertEquals("content_too_large", errorCode(inline));

        JsonObject get = params("path", "/large.bin");
        get.addProperty("exchange_path", "output/large.bin");
        call("get_file", get);
        assertArrayEquals(content, Files.readAllBytes(exchange.resolve("output/large.bin")));

        put.addProperty("exchange_path", "../outside.bin");
        assertEquals("invalid_exchange_path", errorCode(callForResponse("put_file", put)));
    }

    @Test
    void rejectsCorruptContentAndLeavesNoPlaintextOutput() throws Exception {
        initialize();
        call("mkdir", params("path", "/corruption"));
        byte[] cleartext = deterministicBytes(32 * 1024);
        writeInline("/corruption/target.bin", cleartext);
        call("close", new JsonObject());

        Path ciphertext;
        try (var paths = Files.walk(vault.resolve("d"))) {
            ciphertext =
                    paths.filter(Files::isRegularFile)
                            .filter(path -> uncheckedSize(path) > cleartext.length)
                            .max(Comparator.comparingLong(ProtocolServerTest::uncheckedSize))
                            .orElseThrow();
        }
        try (RandomAccessFile file = new RandomAccessFile(ciphertext.toFile(), "rw")) {
            long offset = file.length() - 1;
            file.seek(offset);
            int original = file.readUnsignedByte();
            file.seek(offset);
            file.write(original ^ 0x01);
        }

        open();
        JsonObject read = callForResponse("read_inline", params("path", "/corruption/target.bin"));
        assertEquals("authentication_failed", errorCode(read));

        JsonObject get = params("path", "/corruption/target.bin");
        get.addProperty("exchange_path", "must-not-exist.bin");
        JsonObject exported = callForResponse("get_file", get);
        assertEquals("authentication_failed", errorCode(exported));
        assertFalse(Files.exists(exchange.resolve("must-not-exist.bin")));
    }

    @Test
    void rejectsLogicalTraversalAndRootMutation() {
        initialize();
        assertEquals(
                "invalid_path", errorCode(callForResponse("mkdir", params("path", "/../escape"))));
        assertEquals("invalid_path", errorCode(callForResponse("delete", params("path", "/"))));
    }

    private JsonObject initialize() {
        JsonObject params = params("vault_path", vault.toAbsolutePath().toString());
        params.addProperty("secret", encode(PASSWORD));
        return call("initialize", params);
    }

    private JsonObject open() {
        JsonObject params = params("vault_path", vault.toAbsolutePath().toString());
        params.addProperty("secret", encode(PASSWORD));
        return call("open", params);
    }

    private void writeInline(String path, byte[] content) {
        JsonObject params = params("path", path);
        params.addProperty("content", Base64.getEncoder().encodeToString(content));
        call("write_inline", params);
    }

    private JsonObject call(String method, JsonObject params) {
        JsonObject response = callForResponse(method, params);
        if (response.has("error")) {
            throw new AssertionError(response);
        }
        return response.getAsJsonObject("result");
    }

    private JsonArray callArray(String method, JsonObject params) {
        JsonObject response = callForResponse(method, params);
        if (response.has("error")) {
            throw new AssertionError(response);
        }
        return response.getAsJsonArray("result");
    }

    private JsonObject callForResponse(String method, JsonObject params) {
        JsonObject request = new JsonObject();
        request.addProperty("id", ++requestId);
        request.addProperty("method", method);
        request.add("params", params);
        return server.handle(GSON.toJson(request));
    }

    private static JsonObject params(String name, String value) {
        JsonObject params = new JsonObject();
        params.addProperty(name, value);
        return params;
    }

    private static String errorCode(JsonObject response) {
        return response.getAsJsonObject("error").get("code").getAsString();
    }

    private static String encode(String secret) {
        return Base64.getEncoder().encodeToString(secret.getBytes(StandardCharsets.UTF_8));
    }

    private static boolean hasPath(JsonArray entries, String path) {
        return entries.asList().stream()
                .map(element -> element.getAsJsonObject().get("path").getAsString())
                .anyMatch(path::equals);
    }

    private static JsonObject entryFor(JsonArray entries, String path) {
        return entries.asList().stream()
                .map(element -> element.getAsJsonObject())
                .filter(entry -> entry.get("path").getAsString().equals(path))
                .findFirst()
                .orElseThrow();
    }

    private void assertSafeStoragePaths(JsonArray entries) {
        for (var element : entries) {
            String storagePath = element.getAsJsonObject().get("storage_path").getAsString();
            assertFalse(Path.of(storagePath).isAbsolute());
            assertFalse(storagePath.contains("\\"));
            assertTrue(storagePath.startsWith("d/"));
            for (Path component : Path.of(storagePath)) {
                assertFalse(component.toString().equals(".") || component.toString().equals(".."));
            }
            Path resolved = vault.resolve(storagePath).normalize();
            assertTrue(resolved.startsWith(vault.resolve("d")));
            assertTrue(Files.exists(resolved, LinkOption.NOFOLLOW_LINKS));
        }
    }

    private static byte[] deterministicBytes(int length) {
        byte[] result = new byte[length];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) (31 * index + 17);
        }
        return result;
    }

    private static long uncheckedSize(Path path) {
        try {
            return Files.size(path);
        } catch (IOException exc) {
            throw new AssertionError(exc);
        }
    }
}
