package com.datawizards.dmg.dialects

import com.datawizards.dmg.{DataModelGenerator, DataModelGeneratorBaseTest}
import com.datawizards.dmg.TestModel._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.datawizards.dmg.service.TemplateHandler

@RunWith(classOf[JUnitRunner])
class GenerateFlinkModelTest extends DataModelGeneratorBaseTest {

  test("Simple model") {
    val expected =
      """CREATE TABLE Person(
        |   name STRING,
        |   age INT
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[Person](FlinkDialect)
    }
  }

  test("ClassWithAllSimpleTypes") {
    val expected =
      """CREATE TABLE ClassWithAllSimpleTypes(
        |   strVal STRING,
        |   intVal INT,
        |   longVal BIGINT,
        |   doubleVal DOUBLE,
        |   floatVal FLOAT,
        |   shortVal SMALLINT,
        |   booleanVal BOOLEAN,
        |   byteVal TINYINT,
        |   dateVal DATE,
        |   timestampVal TIMESTAMP
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[ClassWithAllSimpleTypes](FlinkDialect)
    }
  }

  test("Column length") {
    val expected =
      """CREATE TABLE PersonWithCustomLength(
        |   name STRING(1000),
        |   age INT
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[PersonWithCustomLength](FlinkDialect)
    }
  }

  test("Array type") {
    val expected =
      """CREATE TABLE CV(
        |   skills ARRAY<STRING>,
        |   grades ARRAY<INT>
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[CV](FlinkDialect)
    }
  }

  test("Nested array type") {
    val expected =
      """CREATE TABLE NestedArray(
        |   nested ARRAY<ARRAY<STRING>>,
        |   nested3 ARRAY<ARRAY<ARRAY<INT>>>
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[NestedArray](FlinkDialect)
    }
  }

  test("Struct types") {
    val expected =
      """CREATE TABLE Book(
        |   title STRING,
        |   year INT,
        |   owner STRUCT<name : STRING, age : INT>,
        |   authors ARRAY<STRUCT<name : STRING, age : INT>>
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[Book](FlinkDialect)
    }
  }

  test("Multiple table properties") {
    val expected =
      """CREATE TABLE PersonMultipleTableProperties(
        |   name STRING,
        |   age INT
        |)
        |with(
        |'hostname'='localhost',
        |'username'='root',
        |'port'='3306',
        |'connector'='mysql-cdc',
        |'database-name'='dmg',
        |'table-name'='products',
        |'password'='123456');""".stripMargin

    val withMap = Map(
      "connector" -> "mysql-cdc",
      "hostname" -> "localhost",
      "port" -> "3306",
      "username" -> "root",
      "password" -> "123456",
      "database-name" -> "dmg",
      "table-name" -> "products"
    )
    val withString = withMap map {case (key, value) => {s"'${key}'='${value}'"}}
    val withValue = withString.mkString(",\n")
    val data =
      DataModelGenerator.generate[PersonMultipleTableProperties](FlinkDialect)
    val ddl = TemplateHandler.inflate(data, Map("withValue" -> withValue))

    assertResultIgnoringNewLines(expected) {
        ddl
    }
  }

  test("Map type") {
    val expected =
      """CREATE TABLE ClassWithMap(
        |   map MAP<INT, BOOLEAN>
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[ClassWithMap](FlinkDialect)
    }
  }

  test("ClassWithDash") {
    val expected =
      """CREATE TABLE ClassWithDash(
        |   `add-id` STRING
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[ClassWithDash](FlinkDialect)
    }
  }

  test("reserverd keywords") {
    val expected =
      """CREATE TABLE ClassWithReservedKeywords(
        |   `select` STRING,
        |   `where` STRING
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[ClassWithReservedKeywords](FlinkDialect)
    }
  }

  test("ClassWithArrayByte") {
    val expected =
      """CREATE TABLE ClassWithArrayByte(
        |   arr BINARY
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[ClassWithArrayByte](FlinkDialect)
    }
  }

  test("ClassWithBigInteger") {
    val expected =
      """CREATE TABLE ClassWithBigInteger(
        |   n1 BIGINT
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[ClassWithBigInteger](FlinkDialect)
    }
  }

  test("ClassWithBigDecimal") {
    val expected =
      """CREATE TABLE ClassWithBigDecimal(
        |   n1 DECIMAL(38,18)
        |);""".stripMargin

    assertResultIgnoringNewLines(expected) {
      DataModelGenerator.generate[ClassWithBigDecimal](FlinkDialect)
    }
  }

}
