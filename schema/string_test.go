package schema_test

import (
	"fmt"
	"go.arcalot.io/assert"
	"regexp"
	"testing"

	"go.flow.arcalot.io/pluginsdk/schema"
)

func ExampleStringSchema() {
	var stringType schema.String = schema.NewStringSchema(
		schema.IntPointer(5),
		schema.IntPointer(16),
		regexp.MustCompile("^[a-z]+$"),
	)

	// This will fail because it's too short:
	_, err := stringType.Unserialize("abcd")
	fmt.Println(err)

	// This will fail because it's too long:
	_, err = stringType.Unserialize("abcdefghijklmnopqrstuvwxyz")
	fmt.Println(err)

	// This will succeed:
	unserialized, err := stringType.Unserialize("abcde")
	if err != nil {
		panic(err)
	}
	fmt.Println(unserialized)

	// You can validate existing strings:
	err = stringType.Validate("asdf")
	fmt.Println(err)

	// You can also serialize-validate strings:
	serialized, err := stringType.Serialize("asdfg")
	if err != nil {
		panic(err)
	}
	fmt.Println(serialized)

	// Output: Validation failed: String must be at least 5 characters, 4 given
	// Validation failed: String must be at most 16 characters, 26 given
	// abcde
	// Validation failed: String must be at least 5 characters, 4 given
	// asdfg
}

func TestStringTypeAlias(t *testing.T) {
	type T string

	s := schema.NewStringSchema(nil, nil, nil)
	serializedData, err := s.Serialize(T("Hello world!"))
	assert.NoError(t, err)
	assert.Equals(t, serializedData.(string), "Hello world!")
}

func TestStringMinValidation(t *testing.T) {
	stringType := schema.NewStringSchema(schema.IntPointer(5), nil, nil)

	const invalidValue = "asdf"
	const validValue = "asdfg"
	var invalidType = struct{}{}

	testStringSerialization(t, stringType, invalidValue, validValue, invalidType)
}

func TestStringMaxValidation(t *testing.T) {
	stringType := schema.NewStringSchema(nil, schema.IntPointer(4), nil)

	const invalidValue = "asdfg"
	const validValue = "asdf"
	var invalidType = struct{}{}

	testStringSerialization(t, stringType, invalidValue, validValue, invalidType)
}

func TestStringPatternValidation(t *testing.T) {
	stringType := schema.NewStringSchema(nil, nil, regexp.MustCompile("^[a-z]+$"))

	const invalidValue = "asdf1"
	const validValue = "asdf"
	var invalidType = struct{}{}

	testStringSerialization(t, stringType, invalidValue, validValue, invalidType)
}

func TestStringTypeUnserialization(t *testing.T) {
	stringType := schema.NewStringSchema(nil, nil, nil)

	for _, v := range []interface{}{
		3,
		uint(3),
		int64(3),
		uint64(3),
		int32(3),
		uint32(3),
		int16(3),
		uint16(3),
		int8(3),
		uint8(3),
		float32(3),
		float64(3),
	} {
		t.Run(fmt.Sprintf("%v", v), func(t *testing.T) {
			_, err := stringType.Unserialize(v)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func testStringSerialization(
	t *testing.T,
	stringType *schema.StringSchema,
	invalidValue string,
	validValue string,
	invalidType any,
) {
	if _, err := stringType.Unserialize(invalidValue); err == nil {
		t.Fatalf("Unserialize didn't fail on invalid value.")
	}
	if _, err := stringType.Unserialize(invalidType); err == nil {
		t.Fatalf("Unserialize didn't fail on invalid type.")
	}
	if err := stringType.Validate(invalidValue); err == nil {
		t.Fatalf("Validate didn't fail on invalid value.")
	}
	if _, err := stringType.Serialize(invalidValue); err == nil {
		t.Fatalf("Serialize didn't fail on invalid value.")
	}

	val, err := stringType.Unserialize(validValue)
	if err != nil {
		t.Fatalf("Unserialize failed.")
	}
	if val != validValue {
		t.Fatalf("Incorrect value after unserialize: %s", val)
	}
	if err := stringType.Validate(validValue); err != nil {
		t.Fatalf("Validate failed.")
	}
	val2, err := stringType.Serialize(validValue)
	if err != nil {
		t.Fatalf("Serialize failed.")
	}
	if val2 != validValue {
		t.Fatalf("Incorrect value after unserialize: %s", val)
	}
}

func TestStringParameters(t *testing.T) {
	stringType := schema.NewStringSchema(nil, nil, nil)
	assert.Equals(t, stringType.Min(), nil)
	assert.Equals(t, stringType.Max(), nil)
	assert.Equals(t, stringType.Pattern(), nil)

	stringType = schema.NewStringSchema(
		schema.IntPointer(1),
		schema.IntPointer(2),
		regexp.MustCompile("^[a-z]+$"),
	)
	assert.Equals(t, *stringType.Min(), int64(1))
	assert.Equals(t, *stringType.Max(), int64(2))
	assert.Equals(t, (*stringType.Pattern()).String(), "^[a-z]+$")
}

func TestStringID(t *testing.T) {
	assert.Equals(t, schema.NewStringSchema(nil, nil, nil).TypeID(), schema.TypeIDString)
}

func TestVerifyCompatibility(t *testing.T) {
	s1 := schema.NewStringSchema(nil, nil, nil)
	s2 := schema.NewStringSchema(schema.IntPointer(1), schema.IntPointer(3), nil)
	s3 := schema.NewStringSchema(schema.IntPointer(10), schema.IntPointer(15), nil)
	s4 := schema.NewIntSchema(nil, nil, nil)

	assert.NoError(t, s1.ValidateCompatibility(s1))
	assert.NoError(t, s1.ValidateCompatibility(s2))
	assert.NoError(t, s2.ValidateCompatibility(s1))
	assert.NoError(t, s2.ValidateCompatibility(s2))
	assert.NoError(t, s3.ValidateCompatibility(s3))
	assert.Error(t, s2.ValidateCompatibility(s3)) // Mutually exclusive sizes
	assert.Error(t, s3.ValidateCompatibility(s2)) // Mutually exclusive sizes
	assert.Error(t, s1.ValidateCompatibility(s4)) // Wrong type
	assert.NoError(t, s1.ValidateCompatibility("test"))
	assert.Error(t, s1.ValidateCompatibility(1))
	assert.Error(t, s1.ValidateCompatibility([]string{}))
	assert.Error(t, s1.ValidateCompatibility(map[string]string{}))
	assert.Error(t, s2.ValidateCompatibility("This string is too long"))
	assert.Error(t, s2.ValidateCompatibility("short"))
	// A string enum should be valid
	assert.NoError(t, s1.ValidateCompatibility(schema.NewStringEnumSchema(map[string]*schema.DisplayValue{})))
	// Int enum invalid
	assert.Error(t, s1.ValidateCompatibility(schema.NewIntEnumSchema(map[int64]*schema.DisplayValue{}, nil)))
}
