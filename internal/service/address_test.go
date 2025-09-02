package service

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// ---- Tests ----

func TestIsValidEthAddress(t *testing.T) {
	// Arrange
	valid := "0x1111111111111111111111111111111111111111"
	invalid := "12345"

	// Act
	isValid := IsValidEthAddress(valid)
	isInvalid := IsValidEthAddress(invalid)

	// Assert
	if !isValid {
		t.Errorf("expected %s to be valid", valid)
	}
	if isInvalid {
		t.Errorf("expected %s to be invalid", invalid)
	}
}

func TestIsValidUUID(t *testing.T) {
	// Arrange
	validID := uuid.New().String()
	invalidID := "not-a-uuid"

	// Act
	isValid := IsValidUUID(validID)
	isInvalid := IsValidUUID(invalidID)

	// Assert
	if !isValid {
		t.Errorf("expected %s to be valid UUID", validID)
	}
	if isInvalid {
		t.Errorf("expected invalid UUID, got %s", invalidID)
	}
}

func TestLoadUserAddresses_Valid(t *testing.T) {
	// Arrange
	csv := `user_id,address
            550e8400-e29b-41d4-a716-446655440000,0x1111111111111111111111111111111111111111
            f47ac10b-58cc-4372-a567-0e02b2c3d479,0x2222222222222222222222222222222222222222`

	// Act
	recs, err := LoadUserAddresses(strings.NewReader(csv))

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}

	if recs[0].UserID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("wrong userID for first record: %s", recs[0].UserID)
	}
	if recs[0].Address != "0x1111111111111111111111111111111111111111" {
		t.Errorf("wrong address for first record: %s", recs[0].Address)
	}

	if recs[1].UserID != "f47ac10b-58cc-4372-a567-0e02b2c3d479" {
		t.Errorf("wrong userID for second record: %s", recs[1].UserID)
	}
	if recs[1].Address != "0x2222222222222222222222222222222222222222" {
		t.Errorf("wrong address for second record: %s", recs[1].Address)
	}
}

func TestLoadUserAddresses_Errors(t *testing.T) {
	// Arrange
	cases := []struct {
		name string
		csv  string
	}{
		{"empty file", ``},
		{"header only", `user_id,address`},
		{"missing user_id", `user_id,address
        ,0x1111111111111111111111111111111111111111`},
		{"invalid uuid", `user_id,address
        not-a-uuid,0x1111111111111111111111111111111111111111`},
		{"missing address", `user_id,address
        550e8400-e29b-41d4-a716-446655440000,`},
		{"invalid eth address", `user_id,address
        550e8400-e29b-41d4-a716-446655440000,not-an-eth`},
		{"duplicate user_id", `user_id,address
        550e8400-e29b-41d4-a716-446655440000,0x1111111111111111111111111111111111111111
        550e8400-e29b-41d4-a716-446655440000,0x2222222222222222222222222222222222222222`},
	}

	// Act / Assert
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reader := strings.NewReader(c.csv)

			_, err := LoadUserAddresses(reader)

			if err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

func TestLoadUserAddressMapFile(t *testing.T) {
	// Arrange
	content := `user_id,address
                550e8400-e29b-41d4-a716-446655440000,0x1111111111111111111111111111111111111111`
	path := t.TempDir() + "/addrs.csv"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Act
	m, err := LoadUserAddressMapFile(path)

	// Assert
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := m["550e8400-e29b-41d4-a716-446655440000"]; got != "0x1111111111111111111111111111111111111111" {
		t.Errorf("wrong mapping: %s", got)
	}
}

func TestLoadUserAddressMapFile_FileNotFound(t *testing.T) {
	// Arrange
	path := "does-not-exist.csv"

	// Act
	_, err := LoadUserAddressMapFile(path)

	// Assert
	if err == nil {
		t.Error("expected error for missing file")
	}
}
