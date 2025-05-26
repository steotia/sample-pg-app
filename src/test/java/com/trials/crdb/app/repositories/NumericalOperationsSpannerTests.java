package com.trials.crdb.app.repositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public class NumericalOperationsSpannerTests {

    private static final String PROJECT_ID = "emulator-project";
    private static final String INSTANCE_ID = "test-instance";
    private static final String DATABASE_ID = "test-database";
    
    // Create empty credentials file for test
    @BeforeAll
    public static void setupCredentials() throws IOException {
        Files.writeString(Path.of("/tmp/empty-credentials.json"), "{}");
    }
    
    // Create a shared network for containers
    private static final Network NETWORK = Network.newNetwork();

    // Spanner emulator container
    @Container
    static final GenericContainer<?> spannerEmulator = 
        new GenericContainer<>("gcr.io/cloud-spanner-emulator/emulator")
            .withNetwork(NETWORK)
            .withNetworkAliases("spanner-emulator")
            .withExposedPorts(9010, 9020)
            .withStartupTimeout(Duration.ofMinutes(2));
    
    // PGAdapter container with matched configuration
    @Container
    static final GenericContainer<?> pgAdapter = 
        new GenericContainer<>("gcr.io/cloud-spanner-pg-adapter/pgadapter")
            .withNetwork(NETWORK)
            .dependsOn(spannerEmulator)
            .withExposedPorts(5432)
            .withFileSystemBind("/tmp/empty-credentials.json", "/credentials.json", BindMode.READ_ONLY)
            .withCommand(
                "-p", PROJECT_ID,
                "-i", INSTANCE_ID,
                "-d", DATABASE_ID,
                "-e", "spanner-emulator:9010",
                "-c", "/credentials.json",
                "-r", "autoConfigEmulator=true",
                "-x"
            )
            .withStartupTimeout(Duration.ofMinutes(2));

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.datasource.url", () -> {
            String pgHost = pgAdapter.getHost();
            int pgPort = pgAdapter.getMappedPort(5432);
            return String.format("jdbc:postgresql://%s:%d/%s", pgHost, pgPort, DATABASE_ID);
        });
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.datasource.hikari.connection-init-sql", 
            () -> "SET spanner.support_drop_cascade=true");
        registry.add("spring.jpa.properties.hibernate.dialect", 
            () -> "org.hibernate.dialect.PostgreSQLDialect");
        registry.add("spring.jpa.show-sql", () -> "true");
    }

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setupSchema() throws SQLException {
        // Create schema manually for Spanner
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                // Enable Spanner settings
                stmt.execute("SET spanner.support_drop_cascade=true");
                
                // Drop tables if they exist
                stmt.execute("DROP TABLE IF EXISTS numeric_test");
                
                // Create the test table
                stmt.execute(
                    "CREATE TABLE numeric_test (" +
                    "  id INT64 NOT NULL," +
                    "  int_val INT64," +
                    "  decimal_val NUMERIC(10,2)," +
                    "  precise_val NUMERIC(20,10)," +
                    "  float_val FLOAT64," +
                    // Note: Spanner doesn't support arrays of NUMERIC type
                    // numeric_array field is omitted
                    "  PRIMARY KEY (id)" +
                    ")"
                );
                
                // Insert test data
                stmt.execute(
                    "INSERT INTO numeric_test (id, int_val, decimal_val, precise_val, float_val) VALUES " +
                    "(1, 100, 123.45, 12345.6789012345, 123.456)"
                );
                stmt.execute(
                    "INSERT INTO numeric_test (id, int_val, decimal_val, precise_val, float_val) VALUES " +
                    "(2, 200, 987.65, 98765.4321098765, 987.654)"
                );
                stmt.execute(
                    "INSERT INTO numeric_test (id, int_val, decimal_val, precise_val, float_val) VALUES " +
                    "(3, 300, 555.55, 55555.5555555555, 555.555)"
                );
            }
        }
    }

    //-------------------------------------------------------------------------
    // SECTION 1: BASIC ARITHMETIC OPERATIONS
    //-------------------------------------------------------------------------
    
    @Test
    public void testBasicArithmetic() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  id, " +
            "  int_val + 10 AS addition, " +
            "  int_val - 10 AS subtraction, " +
            "  int_val * 2 AS multiplication, " +
            "  int_val / 2 AS division, " +
            "  int_val % 3 AS modulo, " +
            "  POWER(int_val, 2) AS square " +
            "FROM numeric_test " +
            "WHERE id = 1"
        );
        
        Map<String, Object> row = results.get(0);
        assertThat(row.get("addition")).isEqualTo(110);
        assertThat(row.get("subtraction")).isEqualTo(90);
        assertThat(row.get("multiplication")).isEqualTo(200);
        assertThat(row.get("division")).isEqualTo(50);
        assertThat(row.get("modulo")).isEqualTo(1);
        assertThat(((Number)row.get("square")).doubleValue()).isEqualTo(10000.0);
    }
    
    @Test
    public void testOperatorPrecedence() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  id, " +
            "  2 + 3 * 4 AS precedence1, " +
            "  (2 + 3) * 4 AS precedence2, " +
            "  5 - 2 - 1 AS left_associative, " +
            "  5 - (2 - 1) AS parentheses " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        assertThat(row.get("precedence1")).isEqualTo(14); // 2 + (3 * 4)
        assertThat(row.get("precedence2")).isEqualTo(20); // (2 + 3) * 4
        assertThat(row.get("left_associative")).isEqualTo(2); // (5 - 2) - 1
        assertThat(row.get("parentheses")).isEqualTo(4); // 5 - (2 - 1)
    }
    
    @Test
    public void testIntegerVsDecimalDivision() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  5 / 2 AS integer_division, " +
            "  5.0 / 2 AS decimal_division, " +
            "  5 / 2.0 AS mixed_division, " +
            "  CAST(5 AS DECIMAL) / 2 AS cast_division " +
            // "  5 DIV 2 AS integer_div_operator " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        assertThat(row.get("integer_division")).isEqualTo(2); // Integer division truncates in PostgreSQL
        assertThat(((Number)row.get("decimal_division")).doubleValue()).isEqualTo(2.5);
        assertThat(((Number)row.get("mixed_division")).doubleValue()).isEqualTo(2.5);
        assertThat(((Number)row.get("cast_division")).doubleValue()).isEqualTo(2.5);
        // assertThat(row.get("integer_div_operator")).isEqualTo(2); // DIV always produces integer
    }

    //-------------------------------------------------------------------------
    // SECTION 2: PRECISION AND SCALE TESTING
    //-------------------------------------------------------------------------
    
    @Test
    public void testDecimalPrecisionAndScale() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  id, " +
            "  precise_val * 2 AS double_precise, " +
            "  ROUND(precise_val, 2) AS round_to_2, " +
            "  ROUND(precise_val, 0) AS round_to_int, " +
            "  TRUNC(precise_val, 2) AS trunc_to_2, " +
            "  CEIL(precise_val) AS ceiling, " +
            "  FLOOR(precise_val) AS floor " +
            "FROM numeric_test " +
            "WHERE id = 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Check precision is maintained in multiplication
        BigDecimal doublePrecise = toBigDecimal(row.get("double_precise"));
        assertThat(doublePrecise.scale()).isEqualTo(10); // Scale should be preserved
        assertThat(doublePrecise).isEqualTo(new BigDecimal("24691.3578024690"));
        
        // Check rounding behavior
        assertThat(toBigDecimal(row.get("round_to_2"))).isEqualTo(new BigDecimal("12345.68"));
        assertThat(toBigDecimal(row.get("round_to_int"))).isEqualTo(new BigDecimal("12346"));
        
        // Check truncation (no rounding)
        assertThat(toBigDecimal(row.get("trunc_to_2"))).isEqualTo(new BigDecimal("12345.67"));
        
        // Check ceiling and floor
        assertThat(toBigDecimal(row.get("ceiling"))).isEqualTo(new BigDecimal("12346"));
        assertThat(toBigDecimal(row.get("floor"))).isEqualTo(new BigDecimal("12345"));
    }
    
    @Test
    public void testNumericLimits() {
        // Test very large and very small numbers
        jdbcTemplate.execute("DROP TABLE IF EXISTS numeric_limits_test");
        jdbcTemplate.execute(
            "CREATE TABLE numeric_limits_test (" +
            "  id SERIAL PRIMARY KEY," +
            "  large_int BIGINT," +
            "  large_decimal DECIMAL(38,10)," +
            "  small_decimal DECIMAL(38,20)" +
            ")"
        );
        
        // Insert extreme values
        jdbcTemplate.update(
            "INSERT INTO numeric_limits_test (large_int, large_decimal, small_decimal) VALUES " +
            "(9223372036854775807, 9999999999999999999999999999.9999999999, 0.00000000000000000001)"
        );
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT * FROM numeric_limits_test"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Verify max bigint
        assertThat(row.get("large_int")).isEqualTo(Long.MAX_VALUE);
        
        // Verify large decimal
        BigDecimal largeDecimal = toBigDecimal(row.get("large_decimal"));
        assertThat(largeDecimal.precision()).isLessThanOrEqualTo(38);
        assertThat(largeDecimal.toPlainString()).isEqualTo("9999999999999999999999999999.9999999999");
        
        // Verify small decimal
        BigDecimal smallDecimal = toBigDecimal(row.get("small_decimal"));
        assertThat(smallDecimal.scale()).isGreaterThanOrEqualTo(20);
        assertThat(smallDecimal.toPlainString()).isEqualTo("0.00000000000000000001");
    }

    //-------------------------------------------------------------------------
    // SECTION 3: MATHEMATICAL FUNCTIONS
    //-------------------------------------------------------------------------
    
    @Test
    public void testTrigonometricFunctions() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  SIN(RADIANS(30)) AS sin_30, " +
            "  COS(RADIANS(60)) AS cos_60, " +
            "  TAN(RADIANS(45)) AS tan_45, " +
            "  DEGREES(ASIN(0.5)) AS asin_half, " +
            "  DEGREES(ACOS(0.5)) AS acos_half, " +
            "  DEGREES(ATAN(1)) AS atan_1 " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Check common trigonometric values
        assertThat(((Number)row.get("sin_30")).doubleValue()).isCloseTo(0.5, within(0.0001));
        assertThat(((Number)row.get("cos_60")).doubleValue()).isCloseTo(0.5, within(0.0001));
        assertThat(((Number)row.get("tan_45")).doubleValue()).isCloseTo(1.0, within(0.0001));
        assertThat(((Number)row.get("asin_half")).doubleValue()).isCloseTo(30.0, within(0.0001));
        assertThat(((Number)row.get("acos_half")).doubleValue()).isCloseTo(60.0, within(0.0001));
        assertThat(((Number)row.get("atan_1")).doubleValue()).isCloseTo(45.0, within(0.0001));
    }
    
    @Test
    public void testLogarithmicFunctions() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  LOG(2, 8) AS log2_8, " +
            "  LOG(10, 100) AS log10_100, " +
            "  LN(EXP(1)) AS ln_e, " +
            "  LOG10(1000) AS log10_1000, " +
            "  EXP(2) AS exp_2 " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(((Number)row.get("log2_8")).doubleValue()).isCloseTo(3.0, within(0.0001));
        assertThat(((Number)row.get("log10_100")).doubleValue()).isCloseTo(2.0, within(0.0001));
        assertThat(((Number)row.get("ln_e")).doubleValue()).isCloseTo(1.0, within(0.0001));
        assertThat(((Number)row.get("log10_1000")).doubleValue()).isCloseTo(3.0, within(0.0001));
        assertThat(((Number)row.get("exp_2")).doubleValue()).isCloseTo(7.3891, within(0.0001));
    }
    
    @Test
    public void testMiscMathFunctions() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  ABS(-42) AS abs_neg, " +
            "  SIGN(-42) AS sign_neg, " +
            "  SIGN(0) AS sign_zero, " +
            "  SIGN(42) AS sign_pos, " +
            "  SQRT(16) AS sqrt_16, " +
            "  CBRT(27) AS cbrt_27, " +
            "  MOD(10, 3) AS mod_10_3, " +
            "  FACTORIAL(5) AS fact_5 " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(row.get("abs_neg")).isEqualTo(42);
        assertThat(row.get("sign_neg")).isEqualTo(-1.0);
        assertThat(row.get("sign_zero")).isEqualTo(0.0);
        assertThat(row.get("sign_pos")).isEqualTo(1.0);
        assertThat(((Number)row.get("sqrt_16")).doubleValue()).isEqualTo(4.0);
        assertThat(((Number)row.get("cbrt_27")).doubleValue()).isEqualTo(3.0);
        assertThat(row.get("mod_10_3")).isEqualTo(1);
        assertThat(toBigDecimal(row.get("fact_5"))).isEqualTo(new BigDecimal("120"));
    }

    //-------------------------------------------------------------------------
    // SECTION 4: STATISTICAL FUNCTIONS
    //-------------------------------------------------------------------------
    
    @Test
    public void testAggregationFunctions() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  COUNT(*) AS count_all, " +
            "  SUM(int_val) AS sum_int, " +
            "  AVG(decimal_val) AS avg_decimal, " +
            "  MIN(precise_val) AS min_precise, " +
            "  MAX(float_val) AS max_float " +
            "FROM numeric_test"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(row.get("count_all")).isEqualTo(3L);
        assertThat(row.get("sum_int")).isEqualTo(600L);
        assertThat(((Number)row.get("avg_decimal")).doubleValue()).isCloseTo(555.55, within(0.01));
        assertThat(toBigDecimal(row.get("min_precise"))).isEqualTo(new BigDecimal("12345.6789012345"));
        assertThat(((Number)row.get("max_float")).doubleValue()).isCloseTo(987.654, within(0.001));
    }
    
    @Test
    public void testStatisticalFunctions() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  STDDEV_POP(int_val) AS stddev_pop, " +
            "  STDDEV_SAMP(int_val) AS stddev_samp, " +
            "  VAR_POP(int_val) AS var_pop, " +
            "  VAR_SAMP(int_val) AS var_samp " +
            "FROM numeric_test"
        );
        
        Map<String, Object> row = results.get(0);
        
        // These values can be calculated by hand for the sample data
        assertThat(((Number)row.get("stddev_pop")).doubleValue()).isCloseTo(81.65, within(0.01));
        assertThat(((Number)row.get("stddev_samp")).doubleValue()).isCloseTo(100.0, within(0.01));
        assertThat(((Number)row.get("var_pop")).doubleValue()).isCloseTo(6666.67, within(0.01));
        assertThat(((Number)row.get("var_samp")).doubleValue()).isCloseTo(10000.0, within(0.01));
    }
    
    @Test
    public void testOrderedSetAggregates() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY int_val) AS median, " +
            "  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY int_val) AS percentile_25, " +
            "  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY int_val) AS percentile_75, " +
            "  MODE() WITHIN GROUP (ORDER BY int_val) AS mode_val " +
            "FROM numeric_test"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Values for our sample data
        assertThat(((Number)row.get("median")).doubleValue()).isEqualTo(200.0);
        assertThat(((Number)row.get("percentile_25")).doubleValue()).isEqualTo(150.0);
        assertThat(((Number)row.get("percentile_75")).doubleValue()).isEqualTo(250.0);
        assertThat(row.get("mode_val")).isEqualTo(100); // First value in ordered set
    }

    //-------------------------------------------------------------------------
    // SECTION 5: TYPE CASTING AND CONVERSION
    //-------------------------------------------------------------------------
    
    @Test
    public void testTypeCasting() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  CAST(123.45 AS INTEGER) AS decimal_to_int, " +
            "  CAST(123 AS DECIMAL(10,2)) AS int_to_decimal, " +
            "  CAST('123.45' AS DECIMAL(10,2)) AS string_to_decimal, " +
            "  CAST('123' AS INTEGER) AS string_to_int, " +
            "  CAST(123 AS TEXT) AS int_to_string, " +
            // Use PostgreSQL's EXTRACT function instead of direct date-to-int casting
            "  EXTRACT(EPOCH FROM CAST('2022-01-01' AS DATE))::INTEGER AS date_to_int " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(row.get("decimal_to_int")).isEqualTo(123);
        assertThat(toBigDecimal(row.get("int_to_decimal"))).isEqualTo(new BigDecimal("123.00"));
        assertThat(toBigDecimal(row.get("string_to_decimal"))).isEqualTo(new BigDecimal("123.45"));
        assertThat(row.get("string_to_int")).isEqualTo(123);
        assertThat(row.get("int_to_string")).isEqualTo("123");
        // The date_to_int will now contain Unix timestamp for 2022-01-01
        assertThat(row.get("date_to_int")).isNotNull();
    }
    
    @Test
    public void testImplicitConversion() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  123 + 456.78 AS int_plus_decimal, " +
            "  123 || '.45' AS int_concat_string, " +
            "  '123' || 45 AS string_concat_int, " +
            "  '123' = 123 AS string_equals_int " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(((Number)row.get("int_plus_decimal")).doubleValue()).isCloseTo(579.78, within(0.001));
        assertThat(row.get("int_concat_string")).isEqualTo("123.45");
        assertThat(row.get("string_concat_int")).isEqualTo("12345");
        assertThat(row.get("string_equals_int")).isEqualTo(true);
    }

    //-------------------------------------------------------------------------
    // SECTION 6: NUMERIC ARRAYS AND SETS
    //-------------------------------------------------------------------------
    
    @Test
    public void testNumericArrays() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  id, " +
            "  numeric_array[1] AS first_element, " +
            "  numeric_array[2:3] AS slice, " +
            "  array_length(numeric_array, 1) AS array_size, " +
            "  array_append(numeric_array, 100.1) AS appended_array, " +
            "  array_prepend(0.1, numeric_array) AS prepended_array, " +
            "  array_remove(numeric_array, 10.1) AS element_removed " +
            "FROM numeric_test " +
            "WHERE id = 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(toBigDecimal(row.get("first_element"))).isEqualTo(new BigDecimal("10.10"));
        assertThat(row.get("array_size")).isEqualTo(3);
        
        // Check array slice
        Object[] slice = getArrayFromPgArray(row.get("slice"));
        assertThat(slice).hasSize(2);
        assertThat(toBigDecimal(slice[0])).isEqualTo(new BigDecimal("20.20"));
        assertThat(toBigDecimal(slice[1])).isEqualTo(new BigDecimal("30.30"));
        
        // Check array operations
        Object[] appendedArray = getArrayFromPgArray(row.get("appended_array"));
        assertThat(appendedArray).hasSize(4);
        assertThat(toBigDecimal(appendedArray[3])).isEqualTo(new BigDecimal("100.1"));
        
        Object[] prependedArray = getArrayFromPgArray(row.get("prepended_array"));
        assertThat(prependedArray).hasSize(4);
        assertThat(toBigDecimal(prependedArray[0])).isEqualTo(new BigDecimal("0.1"));
        
        Object[] removedArray = getArrayFromPgArray(row.get("element_removed"));
        assertThat(removedArray).hasSize(2);
    }
    
    @Test
    public void testArrayAggregation() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  ARRAY_AGG(int_val ORDER BY id) AS int_array, " +
            "  SUM(numeric_array[1]) AS sum_first_elements, " +
            "  ARRAY(SELECT GENERATE_SERIES(1, 5)) AS generated_array " +
            "FROM numeric_test"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Check array aggregation
        Object[] intArray = getArrayFromPgArray(row.get("int_array"));
        assertThat(intArray).hasSize(3);
        assertThat(intArray[0]).isEqualTo(100);
        assertThat(intArray[1]).isEqualTo(200);
        assertThat(intArray[2]).isEqualTo(300);
        
        // Check sum of array elements
        assertThat(toBigDecimal(row.get("sum_first_elements"))).isEqualTo(new BigDecimal("121.20"));
        
        // Check array generation
        Object[] generatedArray = getArrayFromPgArray(row.get("generated_array"));
        assertThat(generatedArray).hasSize(5);
        assertThat(generatedArray[0]).isEqualTo(1);
        assertThat(generatedArray[4]).isEqualTo(5);
    }

    /**
     * Helper method to convert various numeric representations to BigDecimal for easier comparison
     */
    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof Number) {
            return new BigDecimal(value.toString());
        } else if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to BigDecimal: " + value);
    }
    
    /**
     * Helper method to work with PostgreSQL arrays
     */
    private Object[] getArrayFromPgArray(Object pgArray) {
        if (pgArray instanceof Object[]) {
            return (Object[]) pgArray;
        }
        
        // For PostgreSQL JDBC driver array handling
        try {
            if (pgArray instanceof java.sql.Array) {
                return (Object[]) ((java.sql.Array) pgArray).getArray();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert PostgreSQL array", e);
        }
        
        throw new IllegalArgumentException("Not a PostgreSQL array: " + pgArray);
    }
    
    /**
     * Helper for approximate comparisons
     */
    private org.assertj.core.data.Offset<Double> within(double precision) {
        return org.assertj.core.data.Offset.offset(precision);
    }
}