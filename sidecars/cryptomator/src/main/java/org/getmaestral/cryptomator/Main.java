/*
 * Copyright (C) 2026 The Maestral contributors
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.getmaestral.cryptomator;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class Main {
    private Main() {}

    public static void main(String[] args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        if (args.length == 1 && args[0].equals("--version")) {
            System.out.println(ProtocolServer.SIDECAR_VERSION);
            return;
        }
        if (args.length != 2 || !args[0].equals("--exchange-root")) {
            System.err.println("Usage: maestral-cryptomator-sidecar --exchange-root <private-directory>");
            System.exit(2);
            return;
        }

        try {
            Path exchangeRoot = PathSecurity.requirePrivateExchangeRoot(Path.of(args[1]));
            try (ProtocolServer server = new ProtocolServer(exchangeRoot);
                    BufferedInputStream input = new BufferedInputStream(System.in);
                    PrintWriter output =
                            new PrintWriter(
                                    new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true)) {
                server.serve(input, output);
            }
        } catch (SidecarException exc) {
            System.err.println(exc.getMessage());
            System.exit(2);
        } catch (IOException exc) {
            System.err.println("The Cryptomator sidecar could not start.");
            if (Boolean.getBoolean("maestral.cryptomator.debug")) {
                exc.printStackTrace(System.err);
            }
            System.exit(2);
        }
    }
}
