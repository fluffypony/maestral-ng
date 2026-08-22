
Config files
============

The config files are located at ``$XDG_CONFIG_HOME/maestral`` on Linux (typically
``~/.config/maestral``) and ``~/Library/Application Support/maestral`` on macOS. Each
configuration will get its own INI file with the settings documented below.

Config values for ``path``, ``selective_sync_mode``, and ``selective_sync_paths``
should not be edited manually. Use the corresponding CLI commands or GUI options.
Changes require accompanying actions, such as downloading newly selected items or
moving the local Dropbox directory. Manual edits do not perform those actions.

This also holds for the ``account_id`` which will be written to the config file after
successfully completing the OAuth flow with Dropbox servers.

Any changes will only take effect once Maestral is restarted. Any changes made to the
config file may be overwritten without warning if made while the sync daemon is running.

.. code-block:: ini

    [main]

    # Config file version (not the Maestral version!)
    version = 21.0

    [auth]

    # Unique Dropbox account ID. The account's email
    # address may change and is therefore not stored here.
    account_id = dbid:AABP7CC5bpYd8ghjIColDFrMoc9SdhACA4

    # The keychain to use to store user credentials. If "automatic",
    # will be set automatically from available backends when
    # completing the OAuth flow. Mus be a fully qualified class name.
    keyring = keyring.backends.macOS.Keyring

    [app]

    # Level for notifications from the desktop app:
    # 15 = FILECHANGE
    # 30 = SYNCISSUE
    # 40 = ERROR
    # 100 = NONE
    notification_level = 15

    # Level for log messages:
    # 10 = DEBUG
    # 20 = INFO
    # 30 = WARNING
    # 40 = ERR0R
    log_level = 20

    # Interval in sec to check for updates
    update_notification_interval = 604800

    [sync]

    # The current Dropbox directory
    path = /Users/UserName/Dropbox (Maestral)

    # Interpret selected paths as "exclude" or "include"
    selective_sync_mode = exclude

    # Paths selected by the selective-sync mode
    selective_sync_paths = ['/test_folder', '/sub/folder']

    # Leave local symbolic links unmanaged
    ignore_symlinks = False

    # Interval in sec to perform a full reindexing
    reindex_interval = 604800

    # Maximum CPU usage per core
    max_cpu_percent = 20.0

    # Sync history to keep in seconds
    keep_history = 604800

    # Enable upload syncing
    upload = True

    # Enable download syncing
    download = True
