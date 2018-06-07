//
//  SQLite.swift
//  SQLConnector
//
//  Created by Stefan Urbanek on 2016-05-01.
//  Copyright (c) 2016 Stefan Urbanek. All rights reserved.
//  Licensed under MIT License
//

import Csqlite3
import DatabaseConnector

public typealias SQLiteHandle = OpaquePointer
typealias SQLiteResult = Int32

/// - Returns: a string representing the sqlite return `code`
func sqliteErrorMessage(_ code: SQLiteResult) -> String {
	return String(cString: sqlite3_errstr(code))
}

/// Errors thrown by SQLite
enum SQLiteError: Error {
    case error(String)
    case noRows
    case noColumns

    /// Create an error from a sqlite result value.
    ///
    init(result: SQLiteResult) {
        self = .error(sqliteErrorMessage(result))
    }
}


public class SQLiteConnection: ConnectionProtocol {
	public typealias Cursor = SQLiteCursor

	var handle: SQLiteHandle
    var isClosed: Bool

    /// Create a SQLite connection with a `handle`.
    ///
	public init(handle: SQLiteHandle) {
		self.handle = handle
        self.isClosed = false
	}

    /// Create an in-memory SQLite connection
    public init() throws {
        let result: SQLiteResult
        var handle: SQLiteHandle? = nil

        result = sqlite3_open_v2("",
                                 &handle,
                                 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                                 nil)

        guard let providedHandle = handle else {
            fatalError("sqlite3_open_v2 returned NULL handle")
        }

        switch result {
        case SQLITE_OK:
            self.handle = providedHandle
        default:
            throw SQLiteError(result: result)
        }

        isClosed = false
    }

    deinit {
        if !isClosed {
            close()
        }
    }

	var errorMessage: String {
		return String(cString:sqlite3_errmsg(handle))
	}

    @discardableResult
	public func execute(statement: String) throws -> Cursor {
		var prepared: OpaquePointer? = nil
		var result: Int32

        debugPrint("SQLite execute: \(statement)")

        // We expect SQLite to set the value to non-NULL on success
		result = sqlite3_prepare_v2(handle, statement, -1,
									   &prepared, nil)

        guard result == SQLITE_OK else {
            throw SQLiteError.error(errorMessage)
        }

        let stmt = SQLiteStatement(prepared!)
        let cursor: SQLiteCursor

        if stmt.columnCount == 0 {
            // Execute right away
            try stmt.step()
            
            // Return exhausted cursor
            cursor = SQLiteCursor(stmt, isClosed: true)
        }
        else {
            cursor = SQLiteCursor(stmt)
        }

        return cursor
	}

    public func close() {
        // TODO: Raise exception when trying to close a closed connection
        sqlite3_close_v2(handle)
        isClosed = true
    }
}
