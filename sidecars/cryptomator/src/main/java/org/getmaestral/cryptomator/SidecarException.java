/*
 * Copyright (C) 2026 The Maestral contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.getmaestral.cryptomator;

final class SidecarException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String code;

    SidecarException(String code, String message) {
        super(message);
        this.code = code;
    }

    SidecarException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    String code() {
        return code;
    }
}
