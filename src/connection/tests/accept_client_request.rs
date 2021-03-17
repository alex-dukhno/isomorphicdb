// Copyright 2020 - 2021 Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use futures_lite::future::block_on;
use tempfile::NamedTempFile;

use postgres::wire_protocol::BackendMessage;

use crate::connection::{ConnSupervisor, Encryption, ProtocolConfiguration};
use crate::{
    connection::{
        manager::ConnectionManager,
        network::{Network, TestCase},
    },
    ClientRequest,
};

use super::{certificate_content, pg_frontend};

fn path_to_temp_certificate() -> NamedTempFile {
    let named_temp_file = NamedTempFile::new().expect("Failed to create temporal file");
    let mut file = named_temp_file.reopen().expect("file with content");
    file.write_all(&certificate_content())
        .expect("write certificate content to temp file");
    named_temp_file
}

#[test]
fn trying_read_from_empty_stream() {
    block_on(async {
        let test_case = TestCase::new(vec![]);

        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::none(),
            ConnSupervisor::new(1, 2),
        );

        let result = connection_manager.accept().await;
        assert!(matches!(result, Err(_)));
    });
}

#[test]
fn trying_read_only_length_of_ssl_message() {
    block_on(async {
        let test_case = TestCase::new(vec![&[0, 0, 0, 8], &[]]);

        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::none(),
            ConnSupervisor::new(1, 2),
        );

        let result = connection_manager.accept().await;
        assert!(matches!(result, Err(_)));
    });
}

#[test]
fn sending_reject_notification_for_none_secure() {
    block_on(async {
        let test_case = TestCase::new(vec![pg_frontend::Message::SslRequired.as_vec().as_slice(), &[]]);

        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::none(),
            ConnSupervisor::new(1, 2),
        );

        let result = connection_manager.accept().await;
        assert!(matches!(result, Err(_)));

        let actual_content = test_case.read_result().await;
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(Encryption::RejectSsl.into());
        assert_eq!(actual_content, expected_content);
    });
}

#[test]
fn sending_accept_notification_for_ssl_only_secure() {
    block_on(async {
        let test_case = TestCase::new(vec![pg_frontend::Message::SslRequired.as_vec().as_slice(), &[]]);

        let file = path_to_temp_certificate();
        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::with_ssl(file.path().to_path_buf(), "password".to_owned()),
            ConnSupervisor::new(1, 2),
        );

        let result = connection_manager.accept().await;

        assert!(matches!(result, Err(_)));

        let actual_content = test_case.read_result().await;
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(Encryption::AcceptSsl.into());
        assert_eq!(actual_content, expected_content);
    });
}

#[test]
fn successful_connection_handshake_for_none_secure() {
    block_on(async {
        let test_case = TestCase::new(vec![
            pg_frontend::Message::SslRequired.as_vec().as_slice(),
            pg_frontend::Message::Setup(vec![
                ("user", "username"),
                ("database", "database_name"),
                ("application_name", "psql"),
                ("client_encoding", "UTF8"),
            ])
            .as_vec()
            .as_slice(),
            pg_frontend::Message::Password("123").as_vec().as_slice(),
            &[],
        ]);

        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::none(),
            ConnSupervisor::new(1, 2),
        );

        let result = connection_manager.accept().await;

        assert!(matches!(result, Ok(_)));

        let actual_content = test_case.read_result().await;
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(Encryption::RejectSsl.into());
        expected_content.extend_from_slice(BackendMessage::AuthenticationCleartextPassword.as_vec().as_slice());
        expected_content.extend_from_slice(BackendMessage::AuthenticationOk.as_vec().as_slice());
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("client_encoding".to_owned(), "UTF8".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("DateStyle".to_owned(), "ISO".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("integer_datetimes".to_owned(), "off".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("server_version".to_owned(), "12.4".to_owned())
                .as_vec()
                .as_slice(),
        );

        expected_content.extend_from_slice(BackendMessage::BackendKeyData(1, 0).as_vec().as_slice());
        expected_content.extend_from_slice(BackendMessage::ReadyForQuery.as_vec().as_slice());

        // The random Connection secret key needs to be ignored (set to zero).
        let len = actual_content.len();
        let mut tail = actual_content[len - 6..].to_vec();
        let mut actual_content = actual_content[..len - 10].to_vec();
        actual_content.append(&mut vec![0, 0, 0, 0]);
        actual_content.append(&mut tail);

        assert_eq!(actual_content, expected_content);
    });
}

#[test]
fn successful_connection_handshake_for_ssl_only_secure() {
    block_on(async {
        let test_case = TestCase::new(vec![
            pg_frontend::Message::SslRequired.as_vec().as_slice(),
            pg_frontend::Message::Setup(vec![
                ("user", "username"),
                ("database", "database_name"),
                ("application_name", "psql"),
                ("client_encoding", "UTF8"),
            ])
            .as_vec()
            .as_slice(),
            pg_frontend::Message::Password("123").as_vec().as_slice(),
        ]);

        let file = path_to_temp_certificate();
        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::with_ssl(file.path().to_path_buf(), "password".to_owned()),
            ConnSupervisor::new(1, 2),
        );

        let result = connection_manager.accept().await;

        assert!(matches!(result, Ok(_)));

        let actual_content = test_case.read_result().await;
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(Encryption::AcceptSsl.into());
        expected_content.extend_from_slice(BackendMessage::AuthenticationCleartextPassword.as_vec().as_slice());
        expected_content.extend_from_slice(BackendMessage::AuthenticationOk.as_vec().as_slice());
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("client_encoding".to_owned(), "UTF8".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("DateStyle".to_owned(), "ISO".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("integer_datetimes".to_owned(), "off".to_owned())
                .as_vec()
                .as_slice(),
        );
        expected_content.extend_from_slice(
            BackendMessage::ParameterStatus("server_version".to_owned(), "12.4".to_owned())
                .as_vec()
                .as_slice(),
        );

        expected_content.extend_from_slice(BackendMessage::BackendKeyData(1, 0).as_vec().as_slice());
        expected_content.extend_from_slice(BackendMessage::ReadyForQuery.as_vec().as_slice());

        // The random Connection secret key needs to be ignored (set to zero).
        let len = actual_content.len();
        let mut tail = actual_content[len - 6..].to_vec();
        let mut actual_content = actual_content[..len - 10].to_vec();
        actual_content.append(&mut vec![0, 0, 0, 0]);
        actual_content.append(&mut tail);

        assert_eq!(actual_content, expected_content);
    });
}

#[test]
fn successful_cancel_request_connection() {
    block_on(async {
        let conn_supervisor = ConnSupervisor::new(1, 2);
        let (conn_id, secret_key) = conn_supervisor.alloc().unwrap();

        let test_case = TestCase::new(vec![pg_frontend::Message::CancelRequest(conn_id, secret_key)
            .as_vec()
            .as_slice()]);

        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::none(),
            conn_supervisor,
        );

        let result = connection_manager.accept().await;

        assert!(matches!(result, Ok(Ok(ClientRequest::QueryCancellation(_)))));
    });
}

#[test]
fn verification_failed_cancel_request_connection() {
    block_on(async {
        let conn_supervisor = ConnSupervisor::new(1, 2);
        let (conn_id, secret_key) = conn_supervisor.alloc().unwrap();

        let test_case = TestCase::new(vec![pg_frontend::Message::CancelRequest(conn_id, secret_key + 1)
            .as_vec()
            .as_slice()]);

        let connection_manager = ConnectionManager::new(
            Network::from(test_case.clone()),
            ProtocolConfiguration::none(),
            conn_supervisor,
        );

        let result = connection_manager.accept().await;

        assert!(matches!(result, Ok(Err(()))));
    });
}
