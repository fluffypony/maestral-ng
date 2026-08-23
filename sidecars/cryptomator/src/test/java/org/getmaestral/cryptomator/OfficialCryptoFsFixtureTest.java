/*
 * Copyright (C) 2026 The Maestral contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.getmaestral.cryptomator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OfficialCryptoFsFixtureTest {
    private static final String FIXTURE_ROOT = "fixtures/official-cryptofs-2.10.0-v8-";
    private static final Gson GSON = new Gson();

    @TempDir Path temporaryDirectory;

    private Path exchange;
    private JsonObject manifest;
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
        try (InputStream stream = resource(FIXTURE_ROOT + "manifest.json");
                InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            manifest = GSON.fromJson(reader, JsonObject.class);
        }
    }

    @Test
    void readsThePermanentOfficialCryptoFsFixture() throws Exception {
        Path vault = extract(FIXTURE_ROOT + "valid.zip", temporaryDirectory.resolve("valid"));
        assertEquals(8, manifest.get("vault_format").getAsInt());
        assertEquals(
                "Official org.cryptomator:cryptofs:2.10.0",
                manifest.get("generated_by").getAsString());
        assertEquals(
                manifest.get("valid_zip_sha256").getAsString(),
                sha256(resource(FIXTURE_ROOT + "valid.zip")));

        try (ProtocolServer server = new ProtocolServer(exchange.toRealPath())) {
            open(server, vault);
            JsonObject snapshotParams = new JsonObject();
            snapshotParams.addProperty("include_hash", true);
            JsonArray snapshot = resultArray(call(server, "snapshot", snapshotParams));
            snapshot.forEach(entry -> entry.getAsJsonObject().remove("modified_ms"));
            assertEquals(manifest.getAsJsonArray("logical_snapshot"), snapshot);

            JsonObject link =
                    resultObject(
                            call(
                                    server,
                                    "readlink",
                                    params("path", manifest.get("link_path").getAsString())));
            assertEquals(manifest.get("link_target").getAsString(), link.get("target").getAsString());
            assertTrue(hasPath(snapshot, manifest.get("long_name").getAsString()));
            assertTrue(hasExactPath(snapshot, manifest.get("moved_to").getAsString()));
            assertFalse(hasExactPath(snapshot, manifest.get("moved_from").getAsString()));
        }

        try (var paths = Files.walk(vault.resolve("d"))) {
            assertTrue(paths.anyMatch(path -> path.toString().endsWith(".c9s")));
        }
    }

    @Test
    void refusesThePermanentCorruptFixture() throws Exception {
        Path valid = extract(FIXTURE_ROOT + "valid.zip", temporaryDirectory.resolve("valid"));
        Path corrupt = extract(FIXTURE_ROOT + "corrupt.zip", temporaryDirectory.resolve("corrupt"));
        Path changed = Path.of(manifest.get("corrupt_ciphertext_path").getAsString());
        byte[] validBytes = Files.readAllBytes(valid.resolve(changed));
        byte[] corruptBytes = Files.readAllBytes(corrupt.resolve(changed));
        assertEquals(validBytes.length, corruptBytes.length);
        int differences = 0;
        for (int index = 0; index < validBytes.length; index++) {
            if (validBytes[index] != corruptBytes[index]) {
                differences++;
            }
        }
        assertEquals(1, differences);

        try (ProtocolServer server = new ProtocolServer(exchange.toRealPath())) {
            open(server, corrupt);
            JsonObject response =
                    call(
                            server,
                            "read_inline",
                            params("path", manifest.get("corrupt_logical_path").getAsString()));
            assertEquals(
                    "authentication_failed",
                    response.getAsJsonObject("error").get("code").getAsString());
        }
    }

    private void open(ProtocolServer server, Path vault) {
        JsonObject params = params("vault_path", vault.toAbsolutePath().toString());
        params.addProperty(
                "secret",
                Base64.getEncoder()
                        .encodeToString(
                                manifest.get("password")
                                        .getAsString()
                                        .getBytes(StandardCharsets.UTF_8)));
        resultObject(call(server, "open", params));
    }

    private JsonObject call(ProtocolServer server, String method, JsonObject params) {
        JsonObject request = new JsonObject();
        request.addProperty("id", ++requestId);
        request.addProperty("method", method);
        request.add("params", params);
        return server.handle(GSON.toJson(request));
    }

    private static JsonObject resultObject(JsonObject response) {
        if (response.has("error")) {
            throw new AssertionError(response);
        }
        return response.getAsJsonObject("result");
    }

    private static JsonArray resultArray(JsonObject response) {
        if (response.has("error")) {
            throw new AssertionError(response);
        }
        return response.getAsJsonArray("result");
    }

    private static JsonObject params(String name, String value) {
        JsonObject params = new JsonObject();
        params.addProperty(name, value);
        return params;
    }

    private static boolean hasPath(JsonArray entries, String filename) {
        return entries.asList().stream()
                .map(entry -> entry.getAsJsonObject().get("path").getAsString())
                .anyMatch(path -> path.endsWith("/" + filename));
    }

    private static boolean hasExactPath(JsonArray entries, String expected) {
        return entries.asList().stream()
                .map(entry -> entry.getAsJsonObject().get("path").getAsString())
                .anyMatch(expected::equals);
    }

    private static Path extract(String resourceName, Path destination) throws IOException {
        Files.createDirectory(destination);
        try (InputStream stream = resource(resourceName);
                ZipInputStream archive = new ZipInputStream(stream, StandardCharsets.UTF_8)) {
            ZipEntry entry;
            while ((entry = archive.getNextEntry()) != null) {
                Path target = destination.resolve(entry.getName()).normalize();
                if (!target.startsWith(destination)) {
                    throw new IOException("A fixture entry leaves its extraction root.");
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(archive, target);
                }
                archive.closeEntry();
            }
        }
        return destination;
    }

    private static InputStream resource(String name) {
        InputStream stream = OfficialCryptoFsFixtureTest.class.getClassLoader().getResourceAsStream(name);
        assertNotNull(stream, "Missing fixture " + name);
        return stream;
    }

    private static String sha256(InputStream stream) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exc) {
            throw new AssertionError(exc);
        }
        try (stream) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = stream.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
