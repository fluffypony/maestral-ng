from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests
from dropbox import common, files, sharing, team_common, users, users_common
from dropbox.oauth import DropboxOAuth2FlowNoRedirect

import maestral.client as client_module
from maestral import core
from maestral.client import (
    DropboxClient,
    convert_account,
    convert_full_account,
    convert_metadata,
    convert_shared_link_metadata,
    convert_space_usage,
)
from maestral.exceptions import NotLinkedError, SyncError
from maestral.keyring import CredentialStorage

# ==== DropboxClient tests =============================================================


def test_get_auth_url():
    cred_storage = CredentialStorage("test-config")
    client = DropboxClient("test-config", cred_storage)
    assert client.get_auth_url().startswith("https://")


def test_link():
    cred_storage = Mock(spec_set=CredentialStorage)
    client = DropboxClient("test-config", cred_storage)

    client._auth_flow = Mock(spec_set=DropboxOAuth2FlowNoRedirect)
    client._auth_flow.finish.return_value = Mock(refresh_token="refresh-token")
    account_info = Mock(account_id="account-id")
    client.get_account_info = Mock(return_value=account_info)
    client.update_path_root = Mock()

    res = client.link("code", allow_plaintext_keyring=True)

    assert res == 0
    client.update_path_root.assert_called_once_with(account_info.root_info)
    cred_storage.save_creds.assert_called_once_with(
        "account-id", "refresh-token", allow_plaintext=True
    )


def test_link_error():
    cred_storage = CredentialStorage("test-config")
    client = DropboxClient("test-config", cred_storage)

    with pytest.raises(RuntimeError):
        client.link("code")


def test_link_failed_1():
    cred_storage = CredentialStorage("test-config")
    client = DropboxClient("test-config", cred_storage)

    client._auth_flow = Mock(spec_set=DropboxOAuth2FlowNoRedirect)
    client._auth_flow.finish = Mock(side_effect=requests.exceptions.HTTPError("failed"))

    res = client.link("token")

    assert res == 1


def test_link_failed_2():
    cred_storage = Mock(spec_set=CredentialStorage)
    client = DropboxClient("test-config", cred_storage)

    client._auth_flow = Mock(spec_set=DropboxOAuth2FlowNoRedirect)
    client._auth_flow.finish = Mock(side_effect=ConnectionError("failed"))

    res = client.link("token")

    assert res == 2

    client._auth_flow = Mock(spec_set=DropboxOAuth2FlowNoRedirect)
    client.get_account_info = Mock()
    client.update_path_root = Mock(side_effect=ConnectionError("failed"))

    res = client.link("token")

    assert res == 2


def test_unlink_error():
    cred_storage = CredentialStorage("test-config")
    client = DropboxClient("test-config", cred_storage)

    with pytest.raises(NotLinkedError):
        client.unlink()


def test_retry_regex_reraises_errors_without_string_messages(client):
    calls = 0

    @DropboxClient._retry_on_error(ValueError, max_retries=2, msg_regex="retry")
    def operation(self):
        nonlocal calls
        calls += 1
        raise ValueError()

    with pytest.raises(ValueError):
        operation(client)

    assert calls == 1


def test_throttled_iterators_guard_zero_transfer_count(client, monkeypatch):
    monkeypatch.setattr(client_module.time, "sleep", Mock())
    client.bandwidth_limit_down = 4096
    client.bandwidth_limit_up = 4096
    client.download_chunk_size = 1
    client.upload_chunk_size = 1

    assert list(client._throttled_download_iter(iter([b"a"]))) == [b"a"]
    assert b"".join(client._throttled_upload_iter(b"a")) == b"a"


@pytest.mark.parametrize(
    "allocation",
    [
        users.SpaceAllocation.individual(users.IndividualSpaceAllocation(allocated=0)),
        users.SpaceAllocation.other,
    ],
)
def test_get_space_usage_handles_zero_allocation(client, allocation):
    client._dbx_base = Mock()
    client._dbx_base.users_get_space_usage.return_value = users.SpaceUsage(
        used=10, allocation=allocation
    )

    usage = client.get_space_usage()

    assert usage.used == 10
    assert usage.allocated == 0
    assert "%" not in client._state.get("account", "usage")


def test_create_shared_link_converts_aware_expiry_to_utc(client, monkeypatch):
    result = object()
    client._dbx = Mock()
    client._dbx.sharing_create_shared_link_with_settings.return_value = result
    monkeypatch.setattr(
        client_module, "convert_shared_link_metadata", lambda value: value
    )

    expires = datetime(2026, 1, 2, 12, tzinfo=timezone(timedelta(hours=2)))

    assert client.create_shared_link("/file", expires=expires) is result

    settings = client._dbx.sharing_create_shared_link_with_settings.call_args.args[1]
    assert settings.expires == datetime(2026, 1, 2, 10, tzinfo=timezone.utc)


def _complete_batch(*metadata):
    entries = []

    for value in metadata:
        entry = Mock()
        entry.is_success.return_value = True
        entry.get_success.return_value = SimpleNamespace(metadata=value)
        entries.append(entry)

    result = SimpleNamespace(entries=entries)
    launch = Mock()
    launch.is_complete.return_value = True
    launch.is_async_job_id.return_value = False
    launch.get_complete.return_value = result
    return launch


def _async_batch(job_id="job"):
    launch = Mock()
    launch.is_complete.return_value = False
    launch.is_async_job_id.return_value = True
    launch.get_async_job_id.return_value = job_id
    return launch


def _failed_batch(error):
    status = Mock()
    status.is_in_progress.return_value = False
    status.is_complete.return_value = False
    status.is_failed.return_value = True
    status.get_failed.return_value = error
    return status


def test_remove_batch_keeps_results_aligned_after_failed_chunk(client, monkeypatch):
    client._dbx = Mock()
    client._dbx.files_delete_batch.side_effect = [
        _async_batch(),
        _complete_batch("three", "four"),
    ]
    error = Mock()
    error.is_too_many_write_operations.return_value = False
    client._dbx.files_delete_batch_check.return_value = _failed_batch(error)
    monkeypatch.setattr(client_module, "convert_metadata", lambda value: value)
    monkeypatch.setattr(client_module.time, "sleep", Mock())

    result = client.remove_batch(
        [("/one", None), ("/two", None), ("/three", None), ("/four", None)],
        batch_size=2,
    )

    assert len(result) == 4
    assert [entry.dbx_path for entry in result[:2] if isinstance(entry, SyncError)] == [
        "/one",
        "/two",
    ]
    assert result[2:] == ["three", "four"]


def test_remove_batch_uses_nonzero_poll_interval(client, monkeypatch):
    client._dbx = Mock()
    client._dbx.files_delete_batch.return_value = _async_batch()
    in_progress = Mock()
    in_progress.is_in_progress.return_value = True
    complete = Mock()
    complete.is_in_progress.return_value = False
    complete.is_complete.return_value = True
    complete.get_complete.return_value = _complete_batch("one").get_complete()
    client._dbx.files_delete_batch_check.side_effect = [in_progress, complete]
    sleep = Mock()
    monkeypatch.setattr(client_module, "convert_metadata", lambda value: value)
    monkeypatch.setattr(client_module.time, "sleep", sleep)

    assert client.remove_batch([("/one", None)]) == ["one"]
    assert [args.args[0] for args in sleep.call_args_list] == [0.5, 0.1]


def test_make_dir_batch_preserves_order_when_retrying_chunk(client, monkeypatch):
    client._dbx = Mock()
    client._dbx.files_create_folder_batch.side_effect = [
        _async_batch(),
        _complete_batch("one"),
        _complete_batch("two"),
        _complete_batch("three", "four"),
    ]
    error = Mock()
    error.is_too_many_files.return_value = True
    client._dbx.files_create_folder_batch_check.return_value = _failed_batch(error)
    monkeypatch.setattr(client_module, "convert_metadata", lambda value: value)
    monkeypatch.setattr(client_module.time, "sleep", Mock())

    result = client.make_dir_batch(["/one", "/two", "/three", "/four"], batch_size=2)

    assert result == ["one", "two", "three", "four"]


# ==== type conversion tests ===========================================================


def test_convert_account():
    dbx_account_info = users.Account(
        account_id="1234" * 10,
        name=users.Name(
            given_name="1",
            surname="2",
            display_name="3",
            abbreviated_name="4",
            familiar_name="5",
        ),
        email="mail@musterman.com",
        email_verified=True,
        profile_photo_url="url",
        disabled=False,
    )

    account_info = convert_account(dbx_account_info)

    assert isinstance(account_info, core.Account)
    assert account_info.account_id == "1234" * 10
    assert account_info.display_name == "3"
    assert account_info.email == "mail@musterman.com"
    assert account_info.email_verified is True
    assert account_info.profile_photo_url == "url"


def test_convert_full_account():
    dbx_account_info = users.FullAccount(
        account_id="1234" * 10,
        name=users.Name(
            given_name="1",
            surname="2",
            display_name="3",
            abbreviated_name="4",
            familiar_name="5",
        ),
        email="mail@musterman.com",
        email_verified=True,
        profile_photo_url="url",
        disabled=False,
        country="UK",
        locale="EN_GB",
        team=None,
        team_member_id=None,
        is_paired=False,
        account_type=users_common.AccountType.basic,
        root_info=common.UserRootInfo(
            root_namespace_id="root_id", home_namespace_id="home_id"
        ),
    )

    account_info = convert_full_account(dbx_account_info)

    assert isinstance(account_info, core.FullAccount)
    assert account_info.account_id == "1234" * 10
    assert account_info.display_name == "3"
    assert account_info.email == "mail@musterman.com"
    assert account_info.email_verified is True
    assert account_info.profile_photo_url == "url"
    assert account_info.country == "UK"
    assert account_info.locale == "EN_GB"
    assert account_info.team is None
    assert account_info.team_member_id is None
    assert account_info.account_type is core.AccountType.Basic
    assert account_info.root_info == core.UserRootInfo(
        root_namespace_id="root_id", home_namespace_id="home_id"
    )

    dbx_account_info.account_type = users_common.AccountType.pro

    account_info = convert_full_account(dbx_account_info)

    assert account_info.account_type is core.AccountType.Pro
    assert account_info.root_info == core.UserRootInfo(
        root_namespace_id="root_id", home_namespace_id="home_id"
    )

    dbx_account_info.account_type = users_common.AccountType.business
    dbx_account_info.root_info = common.TeamRootInfo(
        root_namespace_id="root_id", home_namespace_id="home_id", home_path="/home"
    )

    account_info = convert_full_account(dbx_account_info)

    assert account_info.account_type is core.AccountType.Business
    assert account_info.root_info == core.TeamRootInfo(
        root_namespace_id="root_id", home_namespace_id="home_id", home_path="/home"
    )


def test_convert_space_usage_individual():
    dbx_space_usage = users.SpaceUsage(
        used=10,
        allocation=users.SpaceAllocation.individual(
            users.IndividualSpaceAllocation(allocated=20)
        ),
    )

    space_usage = convert_space_usage(dbx_space_usage)

    assert isinstance(space_usage, core.PersonalSpaceUsage)
    assert space_usage.used == 10
    assert space_usage.allocated == 20
    assert space_usage.team_usage is None


def test_convert_space_usage_team():
    dbx_space_usage = users.SpaceUsage(
        used=10,
        allocation=users.SpaceAllocation.team(
            users.TeamSpaceAllocation(
                used=20,
                allocated=30,
                user_within_team_space_allocated=0,
                user_within_team_space_limit_type=team_common.MemberSpaceLimitType.alert_only,
            )
        ),
    )

    space_usage = convert_space_usage(dbx_space_usage)

    assert isinstance(space_usage, core.PersonalSpaceUsage)
    assert space_usage.used == 10
    assert space_usage.allocated == 30
    assert space_usage.team_usage == core.SpaceUsage(20, 30)

    dbx_space_usage = users.SpaceUsage(
        used=10,
        allocation=users.SpaceAllocation.team(
            users.TeamSpaceAllocation(
                used=20,
                allocated=30,
                user_within_team_space_allocated=15,
                user_within_team_space_limit_type=team_common.MemberSpaceLimitType.alert_only,
            )
        ),
    )

    space_usage = convert_space_usage(dbx_space_usage)

    assert isinstance(space_usage, core.PersonalSpaceUsage)
    assert space_usage.used == 10
    assert space_usage.allocated == 15
    assert space_usage.team_usage == core.SpaceUsage(20, 30)


def test_convert_space_usage_other():
    dbx_space_usage = users.SpaceUsage(
        used=10,
        allocation=users.SpaceAllocation.other,
    )

    space_usage = convert_space_usage(dbx_space_usage)

    assert isinstance(space_usage, core.PersonalSpaceUsage)
    assert space_usage.used == 10
    assert space_usage.allocated == 0
    assert space_usage.team_usage is None


def test_convert_metadata_file():
    dbx_md = files.FileMetadata(
        name="Hello",
        path_lower="/folder/hello",
        path_display="/folder/Hello",
        id="id-0123456789",
        client_modified=datetime.utcfromtimestamp(10),
        server_modified=datetime.utcfromtimestamp(20),
        rev="abcdf12687980",
        size=658,
        symlink_info=files.SymlinkInfo(target="/symlink-target"),
        sharing_info=files.FileSharingInfo(
            read_only=False,
            parent_shared_folder_id="parent_shared_folder_id",
            modified_by="dbid-kjahdskjhkljkadsjhjhjmwerjhjhjmwero",
        ),
        is_downloadable=True,
        content_hash="content_hash_hjkglidjsadfjhsdfgkasdhfgocapigkasdhfgociuyoweruqpi",
    )

    md = convert_metadata(dbx_md)

    assert isinstance(md, core.FileMetadata)
    assert md.name == "Hello"
    assert md.path_display == "/folder/Hello"
    assert md.path_lower == "/folder/hello"
    assert md.id == "id-0123456789"
    assert md.client_modified == datetime.fromtimestamp(10, tz=timezone.utc)
    assert md.server_modified == datetime.fromtimestamp(20, tz=timezone.utc)
    assert md.rev == "abcdf12687980"
    assert md.size == 658
    assert md.symlink_target == "/symlink-target"
    assert md.is_downloadable is True
    assert (
        md.content_hash
        == "content_hash_hjkglidjsadfjhsdfgkasdhfgocapigkasdhfgociuyoweruqpi"
    )
    assert md.is_downloadable is True
    assert md.shared is True


def test_convert_metadata_folder():
    dbx_md = files.FolderMetadata(
        name="Hello",
        path_lower="/folder/hello",
        path_display="/folder/Hello",
        id="id-0123456789",
        sharing_info=files.FolderSharingInfo(
            read_only=False,
            parent_shared_folder_id="parent_shared_folder_id",
        ),
    )

    md = convert_metadata(dbx_md)

    assert isinstance(md, core.FolderMetadata)
    assert md.name == "Hello"
    assert md.path_display == "/folder/Hello"
    assert md.path_lower == "/folder/hello"
    assert md.id == "id-0123456789"
    assert md.shared is True


def test_convert_metadata_deleted():
    dbx_md = files.DeletedMetadata(
        name="Hello",
        path_lower="/folder/hello",
        path_display="/folder/Hello",
    )

    md = convert_metadata(dbx_md)

    assert isinstance(md, core.DeletedMetadata)
    assert md.name == "Hello"
    assert md.path_display == "/folder/Hello"
    assert md.path_lower == "/folder/hello"


def test_convert_metadata_unsupported():
    dbx_md = files.Metadata(
        name="Hello",
        path_lower="/folder/hello",
        path_display="/folder/Hello",
    )

    with pytest.raises(RuntimeError):
        convert_metadata(dbx_md)


def test_convert_sharedlink_metdata():
    # Test conversion with effective_audience.

    dbx_md = sharing.SharedLinkMetadata(
        url="/url",
        name="Hello",
        path_lower="/folder/hello",
        expires=datetime.utcfromtimestamp(10),
        link_permissions=sharing.LinkPermissions(
            can_revoke=False,
            effective_audience=sharing.LinkAudience.public,
            link_access_level=sharing.LinkAccessLevel.viewer,
            require_password=True,
            allow_download=True,
        ),
    )

    md = convert_shared_link_metadata(dbx_md)

    assert isinstance(md, core.SharedLinkMetadata)
    assert md.url == "/url"
    assert md.name == "Hello"
    assert md.path_lower == "/folder/hello"
    assert md.expires == datetime.fromtimestamp(10, tz=timezone.utc)
    assert md.link_permissions.require_password is True
    assert md.link_permissions.can_revoke is False
    assert md.link_permissions.allow_download is True
    assert md.link_permissions.link_access_level is core.LinkAccessLevel.Viewer
    assert md.link_permissions.effective_audience is core.LinkAudience.Public

    dbx_md.link_permissions.effective_audience = sharing.LinkAudience.team

    md = convert_shared_link_metadata(dbx_md)

    assert md.link_permissions.effective_audience is core.LinkAudience.Team

    dbx_md.link_permissions.effective_audience = sharing.LinkAudience.no_one

    md = convert_shared_link_metadata(dbx_md)

    assert md.link_permissions.effective_audience is core.LinkAudience.NoOne

    # Test conversion with resolved_visibility.

    dbx_md = sharing.SharedLinkMetadata(
        url="/url",
        name="Hello",
        path_lower="/folder/hello",
        expires=datetime.utcfromtimestamp(10),
        link_permissions=sharing.LinkPermissions(
            can_revoke=False,
            resolved_visibility=sharing.ResolvedVisibility.public,
            link_access_level=sharing.LinkAccessLevel.editor,
            allow_download=True,
        ),
    )

    md = convert_shared_link_metadata(dbx_md)

    assert isinstance(md, core.SharedLinkMetadata)
    assert md.url == "/url"
    assert md.name == "Hello"
    assert md.path_lower == "/folder/hello"
    assert md.expires == datetime.fromtimestamp(10, tz=timezone.utc)
    assert md.link_permissions.require_password is False
    assert md.link_permissions.can_revoke is False
    assert md.link_permissions.allow_download is True
    assert md.link_permissions.link_access_level is core.LinkAccessLevel.Editor
    assert md.link_permissions.effective_audience is core.LinkAudience.Public

    dbx_md.link_permissions.resolved_visibility = sharing.ResolvedVisibility.team_only

    md = convert_shared_link_metadata(dbx_md)

    assert md.link_permissions.effective_audience is core.LinkAudience.Team
    assert md.link_permissions.require_password is False

    dbx_md.link_permissions.resolved_visibility = (
        sharing.ResolvedVisibility.team_and_password
    )

    md = convert_shared_link_metadata(dbx_md)

    assert md.link_permissions.effective_audience is core.LinkAudience.Team
    assert md.link_permissions.require_password is True

    dbx_md.link_permissions.resolved_visibility = sharing.ResolvedVisibility.password

    md = convert_shared_link_metadata(dbx_md)

    assert md.link_permissions.effective_audience is core.LinkAudience.Other
    assert md.link_permissions.require_password is True

    dbx_md.link_permissions.resolved_visibility = sharing.ResolvedVisibility.no_one

    md = convert_shared_link_metadata(dbx_md)

    assert md.link_permissions.effective_audience is core.LinkAudience.NoOne
    assert md.link_permissions.require_password is False
