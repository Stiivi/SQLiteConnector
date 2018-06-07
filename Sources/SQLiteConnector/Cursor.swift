import Csqlite3
import DatabaseConnector

public struct SQLiteRow:Row {
	/// Row owner
	let cursor: SQLiteCursor 
	/// Tuple with row values
	public let values: [Value]

	init(cursor: SQLiteCursor, values: [Value]) {
		self.values = values
		self.cursor = cursor
	}

	public subscript(index: Int) -> Value {
		return values[index]
	}

	public subscript(name: String) -> Value? {
		if let index = cursor.columnIndex(name: name) {
			return values[index]
		}
		else {
			return nil
		}
	}
}


public class SQLiteCursor: CursorProtocol {
	let handle: SQLiteStatement

	public let columnCount: Int
	public let columnNames: [String]
    var isClosed: Bool

	public var rowCount: Int? {
        // SQLite does not report number of rows
		return nil
	}


	init(_ handle: SQLiteStatement, isClosed: Bool=false) {
		self.handle = handle
        self.isClosed = isClosed

        columnCount = handle.columnCount
		columnNames = (0..<columnCount).map {
			index in handle.columnNameAt(index:index)
		}	
	}

	func columnIndex(name: String) -> Int? {
		return self.columnNames.index(of: name)
	}

    /// Fetches a next row from the SQLite prepared statement.
	public func fetchOne() throws -> Row? {
		let values: [Value]
        
        // Return empty forever
        guard !isClosed else {
            return nil
        }

        guard let dataCount = try handle.step() else {
            isClosed = true
            return nil
        }


        if dataCount == columnCount {
            // Construct the row
            try values = (0..<columnCount).map {
                index in try handle.columnValue(Int32(index))
            }	

            return SQLiteRow(cursor:self, values: values)
        }
        else {
            // When this might happen? Schema change?
            fatalError("Column count does not match")
        }
	}

	public func fetchScalar() throws -> Value? {
        guard let row = try fetchOne() else {
            throw SQLiteError.noRows
        }
        guard row.values.count >= 1 else {
            return nil
        }

        // FIXME: Check for number of columns in the row
        return row[0]
	}

    public func close() {
        // FIXME: Implement this
    }

}
