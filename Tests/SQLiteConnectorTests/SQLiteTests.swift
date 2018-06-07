import XCTest

import DatabaseConnector
@testable import SQLiteConnector

class SQLiteTestCase: XCTestCase {

	var connection: SQLiteConnection!

	override func setUp() {
        do {
            connection = try SQLiteConnection()
        }
        catch {
            XCTFail("Unable to connect")
        }
	}

	func testBasicConnect() {
		XCTAssertNotNil(connection)
	}

	func testCursorColumns() throws {
		var cursor = try connection.execute(statement: "SELECT 1 + 1 AS value")

        XCTAssertEqual(cursor.columnCount, 1)
        XCTAssertEqual(cursor.columnNames, ["value"])


		cursor = try connection.execute(statement: "SELECT 1 AS a, 2 as b")
        XCTAssertEqual(cursor.columnCount, 2)
        XCTAssertEqual(cursor.columnNames, ["a", "b"])
	}

	func testNext() throws {
        let cursor = try connection.execute(statement: "SELECT 1 + 1 AS value")

        XCTAssertNotNil(try cursor.fetchOne())

        // No more rows
        XCTAssertNil(try cursor.fetchOne(),
                      "Exhausted cursor should be empty")
        // Any subsequent call to the cursor should yield nil
        XCTAssertNil(try cursor.fetchOne(),
                      "Exhausted cursor should be empty forever")
	}

	func testScalar() throws {
		let cursor = try connection.execute(statement: "SELECT 12 AS value")
        let value = try cursor.fetchScalar() 

        XCTAssertEqual(value!.intValue, 12)
	}

	func testCreateTable() throws {
		let cursor: SQLiteCursor = try connection.execute(statement: "CREATE TABLE data (x NUMBER, y STRING)")
        XCTAssertEqual(cursor.columnCount, 0)
        XCTAssertNil(try cursor.fetchOne())
	}

	func testMultipleRows() throws {
		try connection.execute(statement: "CREATE TABLE data (num NUMBER, label STRING)")
		try connection.execute(statement: "INSERT INTO data VALUES (1, 'one')")
		try connection.execute(statement: "INSERT INTO data VALUES (2, 'two')")
		try connection.execute(statement: "INSERT INTO data VALUES (3, 'three')")

		let cursor = try connection.execute(statement: "SELECT * FROM data")
        XCTAssertEqual(cursor.columnCount, 2)
        XCTAssertEqual(cursor.columnNames, ["num", "label"])

        // Three rows
        XCTAssertNotNil(try cursor.fetchOne())
        XCTAssertNotNil(try cursor.fetchOne())
        XCTAssertNotNil(try cursor.fetchOne())
        XCTAssertNil(try cursor.fetchOne())
        XCTAssertNil(try cursor.fetchOne())
	}

	func testSelectValues() throws {
		try connection.execute(statement: "CREATE TABLE data (num NUMBER, label STRING)")
		try connection.execute(statement: "INSERT INTO data VALUES (1, 'one')")

		let cursor = try connection.execute(statement: "SELECT * FROM data")

        let row = try cursor.fetchOne()!
        // Three rows
        XCTAssertNotNil(row)
        XCTAssertEqual(row[0].intValue, 1)
        XCTAssertEqual(row[1].stringValue, "one")

	}
}
