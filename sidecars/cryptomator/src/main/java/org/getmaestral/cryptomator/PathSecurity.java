/*
 * Copyright (C) 2026 The Maestral contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.getmaestral.cryptomator;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.EnumSet;
import java.util.Set;

final class PathSecurity {
    private static final Set<PosixFilePermission> PRIVATE_DIRECTORY_PERMISSIONS =
            PosixFilePermissions.fromString("rwx------");

    private PathSecurity() {}

    static Path requirePrivateExchangeRoot(Path candidate) throws IOException {
        if (!candidate.isAbsolute()) {
            throw new SidecarException("invalid_exchange_root", "The exchange root must be absolute.");
        }
        if (Files.isSymbolicLink(candidate)) {
            throw new SidecarException("invalid_exchange_root", "The exchange root must not be a link.");
        }
        if (!Files.isDirectory(candidate, LinkOption.NOFOLLOW_LINKS)) {
            throw new SidecarException("invalid_exchange_root", "The exchange root must be a directory.");
        }

        Path realRoot = candidate.toRealPath(LinkOption.NOFOLLOW_LINKS);
        var currentUser =
                FileSystems.getDefault()
                        .getUserPrincipalLookupService()
                        .lookupPrincipalByName(System.getProperty("user.name"));
        if (!Files.getOwner(realRoot, LinkOption.NOFOLLOW_LINKS).equals(currentUser)) {
            throw new SidecarException(
                    "insecure_exchange_root", "The current user must own the exchange root.");
        }
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(realRoot);
            Set<PosixFilePermission> forbidden = EnumSet.noneOf(PosixFilePermission.class);
            forbidden.addAll(permissions);
            forbidden.removeAll(PRIVATE_DIRECTORY_PERMISSIONS);
            if (!forbidden.isEmpty()) {
                throw new SidecarException(
                        "insecure_exchange_root",
                        "The exchange root must not grant access to a group or other users.");
            }
        } catch (UnsupportedOperationException ignored) {
            // Windows has no POSIX mode bits. The Python client creates an owner-only ACL.
        }
        return realRoot;
    }

    static Path resolveExchangeInput(Path exchangeRoot, String relativePath) throws IOException {
        Path path = resolveExchangePath(exchangeRoot, relativePath);
        Path realPath = path.toRealPath();
        if (!realPath.startsWith(exchangeRoot)
                || Files.isSymbolicLink(path)
                || !Files.isRegularFile(realPath, LinkOption.NOFOLLOW_LINKS)) {
            throw new SidecarException("invalid_exchange_path", "The exchange input is not a regular file.");
        }
        return realPath;
    }

    static Path resolveExchangeOutput(Path exchangeRoot, String relativePath) throws IOException {
        Path path = resolveExchangePath(exchangeRoot, relativePath);
        Path parent = path.getParent();
        if (parent == null) {
            throw new SidecarException("invalid_exchange_path", "The exchange output has no parent.");
        }
        createPrivateDirectories(exchangeRoot, parent);
        Path realParent = parent.toRealPath(LinkOption.NOFOLLOW_LINKS);
        if (!realParent.startsWith(exchangeRoot)) {
            throw new SidecarException("invalid_exchange_path", "The exchange output leaves its root.");
        }
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS) && Files.isSymbolicLink(path)) {
            throw new SidecarException("invalid_exchange_path", "The exchange output must not be a link.");
        }
        return path;
    }

    static void setPrivateDirectoryPermissions(Path path) throws IOException {
        try {
            Files.setPosixFilePermissions(path, PRIVATE_DIRECTORY_PERMISSIONS);
        } catch (UnsupportedOperationException ignored) {
            // The caller creates an owner-only ACL on Windows.
        }
    }

    private static Path resolveExchangePath(Path exchangeRoot, String relativePath) {
        if (relativePath.isEmpty()
                || relativePath.indexOf('\0') >= 0
                || relativePath.indexOf('\\') >= 0) {
            throw new SidecarException("invalid_exchange_path", "The exchange path is invalid.");
        }
        Path relative = Path.of(relativePath);
        if (relative.isAbsolute() || relative.normalize().startsWith("..")) {
            throw new SidecarException("invalid_exchange_path", "The exchange path leaves its root.");
        }
        Path resolved = exchangeRoot.resolve(relative).normalize();
        if (!resolved.startsWith(exchangeRoot) || resolved.equals(exchangeRoot)) {
            throw new SidecarException("invalid_exchange_path", "The exchange path leaves its root.");
        }
        return resolved;
    }

    private static void createPrivateDirectories(Path exchangeRoot, Path parent) throws IOException {
        Path relative = exchangeRoot.relativize(parent);
        Path current = exchangeRoot;
        for (Path component : relative) {
            current = current.resolve(component);
            if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                if (Files.isSymbolicLink(current)
                        || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                    throw new SidecarException(
                            "invalid_exchange_path", "An exchange path parent is not a directory.");
                }
            } else {
                Files.createDirectory(current);
                setPrivateDirectoryPermissions(current);
            }
        }
    }
}
