import Csqlite3
import DatabaseConnector

/// Wrapper for `sqlite3_stmt`
struct SQLiteStatement {
	var stmt: OpaquePointer? = nil

	init(_ stmt: OpaquePointer) {
		self.stmt = stmt
	}

	var columnCount: Int {
		return Int(sqlite3_column_count(stmt))
	}

	func columnNameAt(index: Int) -> String {
		let name = sqlite3_column_name(stmt, Int32(index))
        // Name is guaranteed by sqlite to be not NULL
		return String(cString: name!)
	}

    /// Performs a SQLite step on statement and returns number of columns of
    /// the next row or `nil` if the iteration is over.
    ///
    /// Number of columns is 0 for DDL statements.
    ///
    @discardableResult
	func step() throws -> Int? {
		let result: Int32

		result = sqlite3_step(stmt)

		switch result {
		case SQLITE_ROW:
			return Int(sqlite3_data_count(stmt))
		case SQLITE_DONE:
			return nil
		default:
            throw SQLiteError(result: result)
		}
	}

	/// - Returns:  SQLite value for column at index
	func columnValue(_ index: Int32) throws -> Value {
		let type = sqlite3_column_type(stmt, Int32(index))

		switch type {
		case SQLITE_INTEGER:
			let val = sqlite3_column_int(stmt, Int32(index))
			return .int(Int(val))

		case SQLITE_FLOAT:
			let val = sqlite3_column_double(stmt, Int32(index))
			return .double(val)

		case SQLITE_NULL:
			return .null

		case SQLITE3_TEXT, SQLITE_TEXT:
			if let val = sqlite3_column_text(stmt, Int32(index)) {
				let str = String(cString:val)
				return .text(str)
			}
			else {
				return .null
			}

		case SQLITE_BLOB:
            throw SQLiteError.error("Blob is not supported")

		default:
            throw SQLiteError.error("Unknown column type \(type)")
		}
	
	}

    func close() {
        // FIXME: Implement this
    }
}

