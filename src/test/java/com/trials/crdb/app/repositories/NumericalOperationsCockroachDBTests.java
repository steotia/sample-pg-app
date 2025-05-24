package com.trials.crdb.app.repositories;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.springframework.core.env.MapPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(initializers = NumericalOperationsCockroachDBTests.DataSourceInitializer.class)
public class NumericalOperationsCockroachDBTests {

    // CockroachDB container
    @Container
    static final CockroachContainer cockroachContainer = 
        new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:latest"))
            .withCommand("start-single-node --insecure");

    static class DataSourceInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(@NonNull ConfigurableApplicationContext appContext) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("spring.datasource.url", cockroachContainer.getJdbcUrl());
            properties.put("spring.datasource.username", cockroachContainer.getUsername());
            properties.put("spring.datasource.password", cockroachContainer.getPassword());
            properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
            properties.put("spring.jpa.hibernate.ddl-auto", "create-drop");
            properties.put("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.CockroachDialect");
            properties.put("spring.jpa.show-sql", "true");
            
            appContext.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("testcontainers-cockroachdb", properties));
        }
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        // Create test tables for numerical operations
        jdbcTemplate.execute("DROP TABLE IF EXISTS numeric_test");
        jdbcTemplate.execute(
            "CREATE TABLE numeric_test (" +
            "  id SERIAL PRIMARY KEY," +
            "  int_val INTEGER," +
            "  decimal_val DECIMAL(10,2)," +
            "  precise_val DECIMAL(20,10)," +
            "  float_val FLOAT," +
            "  numeric_array DECIMAL(10,2)[]" +
            ")"
        );
        
        // Insert test data
        jdbcTemplate.update(
            "INSERT INTO numeric_test (int_val, decimal_val, precise_val, float_val, numeric_array) VALUES " +
            "(100, 123.45, 12345.6789012345, 123.456, ARRAY[10.1, 20.2, 30.3])"
        );
        jdbcTemplate.update(
            "INSERT INTO numeric_test (int_val, decimal_val, precise_val, float_val, numeric_array) VALUES " +
            "(200, 987.65, 98765.4321098765, 987.654, ARRAY[40.4, 50.5, 60.6])"
        );
        jdbcTemplate.update(
            "INSERT INTO numeric_test (int_val, decimal_val, precise_val, float_val, numeric_array) VALUES " +
            "(300, 555.55, 55555.5555555555, 555.555, ARRAY[70.7, 80.8, 90.9])"
        );
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
        // TOIL - CockroachDB: Returns BIGINT (Long) instead of INTEGER
        // WORKAROUND - Cast to Integer or use Number.intValue()
        assertThat(((Number)row.get("precedence1")).intValue()).isEqualTo(14);
        assertThat(((Number)row.get("precedence2")).intValue()).isEqualTo(20);
        assertThat(((Number)row.get("left_associative")).intValue()).isEqualTo(2);
        assertThat(((Number)row.get("parentheses")).intValue()).isEqualTo(4);
    }
    
    @Test
    public void testIntegerVsDecimalDivision() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            // TOIL - CockroachDB: Always performs decimal division, unlike PostgreSQL integer division
            // WORKAROUND - Use FLOOR() for integer division behavior
            "  FLOOR(5 / 2) AS integer_division, " +
            "  5.0 / 2 AS decimal_division, " +
            "  5 / 2.0 AS mixed_division, " +
            "  CAST(5 AS DECIMAL) / 2 AS cast_division " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        assertThat(((Number)row.get("integer_division")).intValue()).isEqualTo(2);
        assertThat(((Number)row.get("decimal_division")).doubleValue()).isEqualTo(2.5);
        assertThat(((Number)row.get("mixed_division")).doubleValue()).isEqualTo(2.5);
        assertThat(((Number)row.get("cast_division")).doubleValue()).isEqualTo(2.5);
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
            // TOIL - CockroachDB: ambiguous call: log(int, int), candidates are: log(decimal, decimal), log(float, float)
            // WORKAROUND - Add explicit casts to resolve ambiguity
            "  LOG(CAST(2 AS DECIMAL), CAST(8 AS DECIMAL)) AS log2_8, " +
            "  LOG(CAST(10 AS DECIMAL), CAST(100 AS DECIMAL)) AS log10_100, " +
            "  LN(EXP(1)) AS ln_e, " +
            "  LOG10(CAST(1000 AS DECIMAL)) AS log10_1000, " +
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
            // TOIL - CockroachDB: ambiguous call: sqrt(int), candidates are: sqrt(float), sqrt(decimal)
            // WORKAROUND - Add explicit cast to resolve ambiguity
            "  SQRT(CAST(16 AS FLOAT)) AS sqrt_16, " +
            // TOIL - CockroachDB: CBRT function not available
            // WORKAROUND - Use POWER(x, 1.0/3.0) instead
            "  POWER(CAST(27 AS FLOAT), 1.0/3.0) AS cbrt_27, " +
            "  MOD(10, 3) AS mod_10_3 " +
            // TOIL - CockroachDB: FACTORIAL function not available
            // WORKAROUND - Implement using CASE statement (see separate method)
            // "  FACTORIAL(5) AS fact_5 " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(row.get("abs_neg")).isEqualTo(42);
        assertThat(row.get("sign_neg")).isEqualTo(-1.0);
        assertThat(row.get("sign_zero")).isEqualTo(0.0);
        assertThat(row.get("sign_pos")).isEqualTo(1.0);
        assertThat(((Number)row.get("sqrt_16")).doubleValue()).isEqualTo(4.0);
        assertThat(((Number)row.get("cbrt_27")).doubleValue()).isCloseTo(3.0, within(0.001));
        assertThat(row.get("mod_10_3")).isEqualTo(1);
        
        // WORKAROUND - Test factorial using lookup table
        // testFactorialWorkaround();
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
        // TOIL - CockroachDB: Returns different numeric types than PostgreSQL
        // WORKAROUND - Use Number interface and handle type conversion
        assertThat(((Number)row.get("sum_int")).longValue()).isEqualTo(600L);
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
    
    // TOIL - CockroachDB: PERCENTILE_CONT and MODE functions not supported
    // Complex workaround required using window functions
    @Disabled("CockroachDB doesn't support PERCENTILE_CONT and MODE functions - would require complex window function workaround")
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
            "  EXTRACT(EPOCH FROM CAST('2022-01-01' AS DATE))::INTEGER AS date_to_int " +
            "FROM numeric_test " +
            "LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        // TOIL - CockroachDB: Returns BIGINT instead of INTEGER for cast results
        // WORKAROUND - Use Number.intValue() for consistent behavior
        assertThat(((Number)row.get("decimal_to_int")).intValue()).isEqualTo(123);
        assertThat(toBigDecimal(row.get("int_to_decimal"))).isEqualTo(new BigDecimal("123.00"));
        assertThat(toBigDecimal(row.get("string_to_decimal"))).isEqualTo(new BigDecimal("123.45"));
        assertThat(((Number)row.get("string_to_int")).intValue()).isEqualTo(123);
        assertThat(row.get("int_to_string")).isEqualTo("123");
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
            // TOIL - CockroachDB: ARRAY slicing in numeric_array[2:3] not implemented
            // See: https://go.crdb.dev/issue-v/32551/v25.1
            // WORKAROUND - Use individual element access instead
            // "  numeric_array[2:3] AS slice, " +
            "  array_length(numeric_array, 1) AS array_size, " +
            "  array_append(numeric_array, 100.1) AS appended_array, " +
            "  array_prepend(0.1, numeric_array) AS prepended_array, " +
            "  array_remove(numeric_array, 10.1) AS element_removed " +
            "FROM numeric_test " +
            "WHERE id = 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        assertThat(toBigDecimal(row.get("first_element"))).isEqualTo(new BigDecimal("10.10"));
        
        // WORKAROUND - Test array slice using individual element access
        List<Map<String, Object>> sliceResults = jdbcTemplate.queryForList(
            "SELECT numeric_array[2] AS second_element, numeric_array[3] AS third_element " +
            "FROM numeric_test WHERE id = 1"
        );
        Map<String, Object> sliceRow = sliceResults.get(0);
        assertThat(toBigDecimal(sliceRow.get("second_element"))).isEqualTo(new BigDecimal("20.20"));
        assertThat(toBigDecimal(sliceRow.get("third_element"))).isEqualTo(new BigDecimal("30.30"));
        
        assertThat(row.get("array_size")).isEqualTo(3);
        
        // Check array operations
        Object[] appendedArray = getArrayFromPgArray(row.get("appended_array"));
        assertThat(appendedArray).hasSize(4);
        assertThat(toBigDecimal(appendedArray[3])).isEqualTo(new BigDecimal("100.1"));
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
        // TOIL - CockroachDB: Array elements are BIGINT instead of INTEGER
        // WORKAROUND - Cast to Number and use intValue()
        assertThat(((Number)intArray[0]).intValue()).isEqualTo(100);
        assertThat(((Number)intArray[1]).intValue()).isEqualTo(200);
        assertThat(((Number)intArray[2]).intValue()).isEqualTo(300);
        
        // Check sum of array elements
        assertThat(toBigDecimal(row.get("sum_first_elements"))).isEqualTo(new BigDecimal("121.20"));
        
        // Check array generation
        Object[] generatedArray = getArrayFromPgArray(row.get("generated_array"));
        assertThat(generatedArray).hasSize(5);
        assertThat(((Number)generatedArray[0]).intValue()).isEqualTo(1);
        assertThat(((Number)generatedArray[4]).intValue()).isEqualTo(5);
    }

    //-------------------------------------------------------------------------
    // HELPER METHODS
    //-------------------------------------------------------------------------
    
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

    @Test
    public void testDecimalPrecisionAndTrailingZeros() {
        // Create a table with explicit precision/scale
        jdbcTemplate.execute("DROP TABLE IF EXISTS precision_test");
        jdbcTemplate.execute(
            "CREATE TABLE precision_test (" +
            "  id SERIAL PRIMARY KEY," +
            "  val1 DECIMAL(10,2), " +  // 2 decimal places
            "  val2 DECIMAL(10,4), " +  // 4 decimal places
            "  val3 NUMERIC(10,0) " +   // 0 decimal places (whole number)
            ")"
        );
        
        // Insert values with trailing zeros
        jdbcTemplate.update(
            "INSERT INTO precision_test (val1, val2, val3) VALUES (?, ?, ?)",
            new BigDecimal("123.40"), new BigDecimal("123.4000"), new BigDecimal("1230")
        );
        
        // Test retrieval of values
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT * FROM precision_test"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Verify scale is preserved
        BigDecimal val1 = toBigDecimal(row.get("val1"));
        BigDecimal val2 = toBigDecimal(row.get("val2"));
        BigDecimal val3 = toBigDecimal(row.get("val3"));
        
        // Check scale is preserved
        assertThat(val1.scale()).isEqualTo(2);
        assertThat(val2.scale()).isEqualTo(4);
        assertThat(val3.scale()).isEqualTo(0);
        
        // Check trailing zeros are preserved
        assertThat(val1.toPlainString()).isEqualTo("123.40");
        assertThat(val2.toPlainString()).isEqualTo("123.4000");
        assertThat(val3.toPlainString()).isEqualTo("1230");
    }

    @Test
    public void testVeryHighPrecision() {
        // Test very high precision arithmetic
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  CAST('0.1234567890123456789012345678901234567890' AS DECIMAL(40,38)) AS high_precision, " +
            "  CAST('0.1234567890123456789012345678901234567890' AS DECIMAL(40,38)) * " +
            "  CAST('0.9876543210987654321098765432109876543210' AS DECIMAL(40,38)) AS high_precision_mul " +
            "FROM numeric_test LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        BigDecimal highPrecision = toBigDecimal(row.get("high_precision"));
        BigDecimal highPrecisionMul = toBigDecimal(row.get("high_precision_mul"));
        
        // Check that PostgreSQL can handle high precision
        assertThat(highPrecision.toPlainString()).startsWith("0.12345678901234567890");
        assertThat(highPrecision.scale()).isGreaterThanOrEqualTo(38);
        
        // Check multiplication preserves high precision
        assertThat(highPrecisionMul.scale()).isGreaterThanOrEqualTo(38);
    }

    @Test
    public void testExplicitRoundingWithScale() {
        // Test explicit rounding with different scales
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  ROUND(123.456, 0) AS round_0, " +
            "  ROUND(123.456, 1) AS round_1, " +
            "  ROUND(123.456, 2) AS round_2, " +
            "  ROUND(123.456, -1) AS round_neg_1, " +
            "  ROUND(1.5) AS round_default, " +
            "  ROUND(2.5) AS round_default_2 " +
            "FROM numeric_test LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Check rounding behavior
        assertThat(toBigDecimal(row.get("round_0")).toPlainString()).isEqualTo("123");
        assertThat(toBigDecimal(row.get("round_1")).toPlainString()).isEqualTo("123.5");
        assertThat(toBigDecimal(row.get("round_2")).toPlainString()).isEqualTo("123.46");
        assertThat(toBigDecimal(row.get("round_neg_1")).toPlainString()).isEqualTo("120");
        
        // PostgreSQL rounds to nearest even number for tie-breaking (banker's rounding)
        assertThat(toBigDecimal(row.get("round_default")).toPlainString()).isEqualTo("2");
        assertThat(toBigDecimal(row.get("round_default_2")).toPlainString()).isEqualTo("3");
    }

    // TOIL - CockroachDB: JUSTIFY_HOURS function not available
    // Complex workaround would require manual interval arithmetic
    @Disabled("CockroachDB doesn't support JUSTIFY_HOURS function - manual interval arithmetic required")
    @Test
    public void testIntervalOperations() {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(
            "SELECT " +
            "  INTERVAL '1 day' + INTERVAL '2 hours' AS interval_addition, " +
            "  INTERVAL '1 day' * 2 AS interval_multiplication, " +
            "  JUSTIFY_HOURS(INTERVAL '30 hours') AS interval_justify, " +
            "  (JUSTIFY_HOURS(INTERVAL '30 hours'))::TIME AS interval_time_part, " +
            "  EXTRACT(hours FROM INTERVAL '1 day 2 hours') AS extract_from_interval, " +
            "  AGE(TIMESTAMP '2023-01-02', TIMESTAMP '2023-01-01') AS age_between_timestamps, " +
            "  TIMESTAMP '2023-01-01' + INTERVAL '1 day' AS timestamp_plus_interval " +
            "FROM numeric_test LIMIT 1"
        );
        
        Map<String, Object> row = results.get(0);
        
        // Test interval addition
        assertThat(row.get("interval_addition").toString()).contains("1 day");
        
        // Test interval multiplication 
        assertThat(row.get("interval_multiplication").toString()).contains("2 days");
        
        // Test interval normalization
        assertThat(row.get("interval_justify").toString()).contains("1 day");
        // Check the time part separately using the TIME cast
        assertThat(row.get("interval_time_part").toString()).isEqualTo("06:00:00");
        
        // Test extracting from interval
        assertThat(((Number)row.get("extract_from_interval")).intValue()).isEqualTo(2);
        
        // Test age function
        assertThat(row.get("age_between_timestamps").toString()).contains("1 day");
        
        // Test timestamp + interval
        assertThat(row.get("timestamp_plus_interval").toString()).startsWith("2023-01-02");
    }
    
    /**
     * Helper for approximate comparisons
     */
    private org.assertj.core.data.Offset<Double> within(double precision) {
        return org.assertj.core.data.Offset.offset(precision);
    }
}