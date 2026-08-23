/*
 * Copyright (C) 2026 The Maestral contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.getmaestral.cryptomator;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

import org.cryptomator.cryptofs.VaultConfigLoadException;
import org.cryptomator.cryptolib.api.AuthenticationFailedException;
import org.cryptomator.cryptolib.api.InvalidPassphraseException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

final class ProtocolServer implements AutoCloseable {
    static final int PROTOCOL_VERSION = 2;
    static final String SIDECAR_VERSION = "0.2.0";
    static final String CRYPTOFS_VERSION = "2.10.0";
    static final String CRYPTOLIB_VERSION = "2.2.2";

    private static final int MAX_REQUEST_BYTES = 2 * 1024 * 1024;
    private static final int MAX_SECRET_BYTES = 4096;
    private static final int DEFAULT_PAGE_ENTRIES = 4096;
    private static final int MAX_PAGE_ENTRIES = 4096;
    private static final int MAX_PAGE_BYTES = 8 * 1024 * 1024;
    private static final int MAX_ACTIVE_PAGES = 8;
    private static final Gson GSON = new Gson();

    private final Path exchangeRoot;
    private final Map<String, PageState> pages = new HashMap<>();
    private VaultSession session;
    private boolean shutdown;

    ProtocolServer(Path exchangeRoot) {
        this.exchangeRoot = exchangeRoot;
    }

    void serve(InputStream input, PrintWriter output) throws IOException {
        while (!shutdown) {
            String line;
            try {
                line = readLine(input);
            } catch (SidecarException exc) {
                output.println(GSON.toJson(error(JsonNull.INSTANCE, exc.code(), exc.getMessage())));
                continue;
            }
            if (line == null) {
                return;
            }
            if (line.isBlank()) {
                output.println(
                        GSON.toJson(
                                error(
                                        JsonNull.INSTANCE,
                                        "invalid_request",
                                        "A request must not be empty.")));
                continue;
            }
            output.println(GSON.toJson(handle(line)));
        }
    }

    JsonObject handle(String line) {
        JsonElement id = JsonNull.INSTANCE;
        try {
            JsonElement parsed = JsonParser.parseString(line);
            if (!parsed.isJsonObject()) {
                throw new SidecarException("invalid_request", "A request must be a JSON object.");
            }
            JsonObject request = parsed.getAsJsonObject();
            id = requireId(request);
            String method = requireString(request, "method");
            JsonObject params = optionalObject(request, "params");
            JsonElement result = dispatch(method, params);
            return success(id, result);
        } catch (Exception exc) {
            ErrorInfo mapped = mapError(exc);
            return error(id, mapped.code(), mapped.message());
        }
    }

    @Override
    public void close() throws IOException {
        pages.clear();
        if (session != null) {
            VaultSession openSession = session;
            session = null;
            openSession.close();
        }
    }

    private JsonElement dispatch(String method, JsonObject params) throws IOException {
        return switch (method) {
            case "hello" -> hello();
            case "initialize" -> initialize(params);
            case "open" -> open(params);
            case "close" -> closeVault();
            case "vault_info" -> requireSession().info();
            case "stat" ->
                    requireSession()
                            .stat(
                                    requireString(params, "path"),
                                    optionalBoolean(params, "include_hash", false));
            case "list" ->
                    requireSession()
                            .list(
                                    requireString(params, "path"),
                                    optionalBoolean(params, "include_hash", false));
            case "snapshot" ->
                    paged(
                            "snapshot",
                            params,
                            () ->
                                    requireSession()
                                            .snapshot(
                                                    optionalBoolean(
                                                            params, "include_hash", false)));
            case "storage_map" ->
                    paged("storage_map", params, () -> requireSession().storageMap());
            case "mkdir" -> {
                requireSession()
                        .makeDirectory(
                                requireString(params, "path"),
                                optionalBoolean(params, "parents", false));
                yield ok();
            }
            case "put_file" -> {
                requireSession()
                        .putFile(
                                requireString(params, "path"),
                                requireString(params, "exchange_path"),
                                optionalLong(params, "modified_ms"),
                                optionalBoolean(params, "replace", false));
                yield ok();
            }
            case "write_inline" -> {
                requireSession()
                        .writeInline(
                                requireString(params, "path"),
                                requireString(params, "content"),
                                optionalLong(params, "modified_ms"),
                                optionalBoolean(params, "replace", false));
                yield ok();
            }
            case "get_file" -> {
                requireSession()
                        .getFile(
                                requireString(params, "path"),
                                requireString(params, "exchange_path"),
                                optionalBoolean(params, "replace", false));
                yield ok();
            }
            case "read_inline" -> {
                JsonObject result = new JsonObject();
                result.addProperty(
                        "content", requireSession().readInline(requireString(params, "path")));
                yield result;
            }
            case "move" -> {
                requireSession()
                        .move(
                                requireString(params, "source"),
                                requireString(params, "target"),
                                optionalBoolean(params, "replace", false));
                yield ok();
            }
            case "delete" -> {
                requireSession()
                        .delete(
                                requireString(params, "path"),
                                optionalBoolean(params, "recursive", false));
                yield ok();
            }
            case "symlink" -> {
                requireSession()
                        .createLink(requireString(params, "path"), requireString(params, "target"));
                yield ok();
            }
            case "readlink" -> {
                JsonObject result = new JsonObject();
                result.addProperty(
                        "target", requireSession().readLink(requireString(params, "path")));
                yield result;
            }
            case "shutdown" -> shutdown();
            default -> throw new SidecarException("unknown_method", "The method is not supported.");
        };
    }

    private JsonObject hello() {
        JsonObject result = new JsonObject();
        result.addProperty("protocol_version", PROTOCOL_VERSION);
        result.addProperty("sidecar_version", SIDECAR_VERSION);
        result.addProperty("cryptofs_version", CRYPTOFS_VERSION);
        result.addProperty("cryptolib_version", CRYPTOLIB_VERSION);
        result.addProperty("vault_format", 8);
        result.addProperty("max_inline_bytes", VaultSession.MAX_INLINE_BYTES);
        result.addProperty("vault_open", session != null);
        return result;
    }

    private JsonObject initialize(JsonObject params) throws IOException {
        requireNoSession();
        char[] passphrase = decodeSecret(requireString(params, "secret"));
        try {
            session =
                    VaultSession.initialize(
                            exchangeRoot, Path.of(requireString(params, "vault_path")), passphrase);
            return session.info();
        } finally {
            Arrays.fill(passphrase, '\0');
        }
    }

    private JsonObject open(JsonObject params) throws IOException {
        requireNoSession();
        char[] passphrase = decodeSecret(requireString(params, "secret"));
        try {
            session =
                    VaultSession.open(
                            exchangeRoot, Path.of(requireString(params, "vault_path")), passphrase);
            return session.info();
        } finally {
            Arrays.fill(passphrase, '\0');
        }
    }

    private JsonObject closeVault() throws IOException {
        VaultSession openSession = requireSession();
        session = null;
        pages.clear();
        openSession.close();
        return ok();
    }

    private JsonObject shutdown() throws IOException {
        close();
        shutdown = true;
        return ok();
    }

    private void requireNoSession() {
        if (session != null) {
            throw new SidecarException("vault_already_open", "Close the open vault first.");
        }
    }

    private VaultSession requireSession() {
        if (session == null) {
            throw new SidecarException("vault_not_open", "Open a vault first.");
        }
        return session;
    }

    private JsonObject paged(String method, JsonObject params, PageLoader loader)
            throws IOException {
        String cursor = optionalString(params, "cursor");
        PageState state;
        if (cursor == null) {
            if (pages.size() >= MAX_ACTIVE_PAGES) {
                throw new SidecarException(
                        "cursor_limit", "Too many paged results are still active.");
            }
            int limit = optionalPositiveInt(params, "limit", DEFAULT_PAGE_ENTRIES);
            state = new PageState(method, loader.load(), 0, limit);
        } else {
            state = pages.get(cursor);
            if (state == null || !state.method().equals(method)) {
                throw new SidecarException("invalid_cursor", "The page cursor is invalid.");
            }
        }

        JsonArray page = new JsonArray();
        int index = state.offset();
        int encodedBytes = 2;
        while (index < state.entries().size() && page.size() < state.limit()) {
            JsonElement entry = state.entries().get(index);
            int entryBytes = GSON.toJson(entry).getBytes(StandardCharsets.UTF_8).length + 1;
            if (!page.isEmpty() && encodedBytes + entryBytes > MAX_PAGE_BYTES) {
                break;
            }
            page.add(entry);
            encodedBytes += entryBytes;
            index++;
        }

        String nextCursor = cursor;
        if (index < state.entries().size()) {
            if (nextCursor == null) {
                nextCursor = UUID.randomUUID().toString();
            }
            pages.put(
                    nextCursor,
                    new PageState(state.method(), state.entries(), index, state.limit()));
        } else if (nextCursor != null) {
            pages.remove(nextCursor);
            nextCursor = null;
        }

        JsonObject result = new JsonObject();
        result.add("entries", page);
        if (nextCursor == null) {
            result.add("next_cursor", JsonNull.INSTANCE);
        } else {
            result.addProperty("next_cursor", nextCursor);
        }
        return result;
    }

    private static char[] decodeSecret(String encoded) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException exc) {
            throw new SidecarException("invalid_secret", "The secret is not valid base64.");
        }
        if (decoded.length == 0 || decoded.length > MAX_SECRET_BYTES) {
            Arrays.fill(decoded, (byte) 0);
            throw new SidecarException("invalid_secret", "The secret length is invalid.");
        }

        CharBuffer decodedChars = null;
        try {
            decodedChars =
                    StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(decoded));
            char[] secret = new char[decodedChars.remaining()];
            decodedChars.get(secret);
            return secret;
        } catch (CharacterCodingException exc) {
            throw new SidecarException("invalid_secret", "The secret is not valid UTF-8.");
        } finally {
            Arrays.fill(decoded, (byte) 0);
            if (decodedChars != null && decodedChars.hasArray()) {
                Arrays.fill(decodedChars.array(), '\0');
            }
        }
    }

    private static JsonElement requireId(JsonObject request) {
        JsonElement id = request.get("id");
        if (id == null || !id.isJsonPrimitive()) {
            throw new SidecarException("invalid_request", "A request needs a string or number ID.");
        }
        var primitive = id.getAsJsonPrimitive();
        if (!primitive.isString() && !primitive.isNumber()) {
            throw new SidecarException("invalid_request", "A request needs a string or number ID.");
        }
        return id;
    }

    private static String requireString(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new SidecarException(
                    "invalid_request", "The " + name + " field must be a string.");
        }
        return value.getAsString();
    }

    private static JsonObject optionalObject(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return new JsonObject();
        }
        if (!value.isJsonObject()) {
            throw new SidecarException(
                    "invalid_request", "The " + name + " field must be an object.");
        }
        return value.getAsJsonObject();
    }

    private static String optionalString(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new SidecarException(
                    "invalid_request", "The " + name + " field must be a string.");
        }
        return value.getAsString();
    }

    private static int optionalPositiveInt(JsonObject object, String name, int defaultValue) {
        Long value = optionalLong(object, name);
        if (value == null) {
            return defaultValue;
        }
        if (value <= 0 || value > MAX_PAGE_ENTRIES) {
            throw new SidecarException(
                    "invalid_request",
                    "The " + name + " field must be between 1 and " + MAX_PAGE_ENTRIES + ".");
        }
        return value.intValue();
    }

    private static boolean optionalBoolean(JsonObject object, String name, boolean defaultValue) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return defaultValue;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isBoolean()) {
            throw new SidecarException(
                    "invalid_request", "The " + name + " field must be a boolean.");
        }
        return value.getAsBoolean();
    }

    private static Long optionalLong(JsonObject object, String name) {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new SidecarException(
                    "invalid_request", "The " + name + " field must be an integer.");
        }
        try {
            return value.getAsBigDecimal().longValueExact();
        } catch (ArithmeticException | NumberFormatException exc) {
            throw new SidecarException(
                    "invalid_request", "The " + name + " field must be an integer.");
        }
    }

    private static JsonObject ok() {
        JsonObject result = new JsonObject();
        result.addProperty("ok", true);
        return result;
    }

    private static JsonObject success(JsonElement id, JsonElement result) {
        JsonObject response = new JsonObject();
        response.add("id", id);
        response.add("result", result);
        return response;
    }

    private static JsonObject error(JsonElement id, String code, String message) {
        JsonObject details = new JsonObject();
        details.addProperty("code", code);
        details.addProperty("message", message);
        JsonObject response = new JsonObject();
        response.add("id", id);
        response.add("error", details);
        return response;
    }

    private static ErrorInfo mapError(Exception exception) {
        if (exception instanceof SidecarException sidecar) {
            return new ErrorInfo(sidecar.code(), sidecar.getMessage());
        }
        if (containsCause(exception, InvalidPassphraseException.class)) {
            return new ErrorInfo("invalid_passphrase", "The vault password is incorrect.");
        }
        if (containsCause(exception, AuthenticationFailedException.class)) {
            return new ErrorInfo(
                    "authentication_failed", "Cryptomator rejected unauthenticated content.");
        }
        if (containsCause(exception, VaultConfigLoadException.class)) {
            return new ErrorInfo("invalid_vault", "The signed vault configuration is invalid.");
        }
        if (exception instanceof JsonParseException) {
            return new ErrorInfo("invalid_json", "The request is not valid JSON.");
        }
        if (exception instanceof NoSuchFileException) {
            return new ErrorInfo("not_found", "The requested path does not exist.");
        }
        if (exception instanceof FileAlreadyExistsException) {
            return new ErrorInfo("already_exists", "The destination already exists.");
        }
        if (exception instanceof NotDirectoryException) {
            return new ErrorInfo("not_directory", "The requested path is not a directory.");
        }
        if (exception instanceof DirectoryNotEmptyException) {
            return new ErrorInfo("directory_not_empty", "The directory is not empty.");
        }
        if (exception instanceof AccessDeniedException) {
            return new ErrorInfo("access_denied", "The process cannot access the requested path.");
        }
        if (exception instanceof IOException) {
            return new ErrorInfo("io_error", "A filesystem operation failed.");
        }
        if (Boolean.getBoolean("maestral.cryptomator.debug")) {
            exception.printStackTrace(System.err);
        }
        return new ErrorInfo("internal_error", "The sidecar could not complete the request.");
    }

    private static boolean containsCause(Throwable exception, Class<? extends Throwable> type) {
        Throwable current = exception;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String readLine(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        boolean oversized = false;
        while (true) {
            int next = input.read();
            if (next == -1) {
                if (bytes.size() == 0 && !oversized) {
                    return null;
                }
                break;
            }
            if (next == '\n') {
                break;
            }
            if (bytes.size() < MAX_REQUEST_BYTES) {
                bytes.write(next);
            } else {
                oversized = true;
            }
        }
        if (oversized) {
            throw new SidecarException("request_too_large", "The request is too large.");
        }
        byte[] encoded = bytes.toByteArray();
        int length = encoded.length;
        if (length > 0 && encoded[length - 1] == '\r') {
            length--;
        }
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded, 0, length))
                    .toString();
        } catch (CharacterCodingException exc) {
            throw new SidecarException("invalid_utf8", "The request is not valid UTF-8.");
        } finally {
            Arrays.fill(encoded, (byte) 0);
        }
    }

    @FunctionalInterface
    private interface PageLoader {
        JsonArray load() throws IOException;
    }

    private record PageState(String method, JsonArray entries, int offset, int limit) {}

    private record ErrorInfo(String code, String message) {}
}
