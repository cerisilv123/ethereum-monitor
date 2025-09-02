package service

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

type UserAddress struct {
	UserID  string
	Address string
}

var ethAddrRe = regexp.MustCompile(`^(?i)0x[0-9a-f]{40}$`)

func IsValidEthAddress(s string) bool {
	return ethAddrRe.MatchString(s)
}

func LoadUserAddressesFile(path string) ([]UserAddress, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()
	return LoadUserAddresses(f)
}

func LoadUserAddressMapFile(path string) (map[string]string, error) {
	recs, err := LoadUserAddressesFile(path)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(recs))
	for _, r := range recs {
		out[r.UserID] = r.Address
	}
	return out, nil
}

func LoadUserAddresses(r io.Reader) ([]UserAddress, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true

	var (
		recs          []UserAddress
		seenUserIDs   = make(map[string]int)
		headerHandled bool
		lineNum       int
	)

	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		lineNum++
		if err != nil {
			return nil, fmt.Errorf("csv read error on line %d: %w", lineNum, err)
		}

		for i := range row {
			row[i] = strings.TrimSpace(row[i])
		}
		if allEmpty(row) || isComment(row) {
			continue
		}

		if !headerHandled {
			if err := checkHeader(row, lineNum); err != nil {
				return nil, err
			}
			headerHandled = true
			continue
		}

		if len(row) < 2 {
			return nil, fmt.Errorf("line %d: expected user_id,address; got %d cols", lineNum, len(row))
		}

		userID := row[0]
		addr := row[1]

		if userID == "" {
			return nil, fmt.Errorf("line %d: user_id is empty", lineNum)
		}
		if !IsValidUUID(userID) {
			return nil, fmt.Errorf("line %d: invalid UUID %q", lineNum, userID)
		}
		if addr == "" {
			return nil, fmt.Errorf("line %d: address is empty for user_id %q", lineNum, userID)
		}
		if !IsValidEthAddress(addr) {
			return nil, fmt.Errorf("line %d: invalid Ethereum address %q", lineNum, addr)
		}
		if firstLine, dup := seenUserIDs[userID]; dup {
			return nil, fmt.Errorf("line %d: duplicate user_id %q (first seen line %d)", lineNum, userID, firstLine)
		}
		seenUserIDs[userID] = lineNum

		recs = append(recs, UserAddress{
			UserID:  userID,
			Address: addr,
		})
	}

	if len(recs) == 0 {
		return nil, errors.New("no address rows found")
	}
	return recs, nil
}

// --- helpers ---

func IsValidUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

func allEmpty(row []string) bool {
	for _, c := range row {
		if strings.TrimSpace(c) != "" {
			return false
		}
	}
	return true
}

func isComment(row []string) bool {
	return len(row) == 1 && strings.HasPrefix(strings.TrimSpace(row[0]), "#")
}

func checkHeader(row []string, lineNum int) error {
	if len(row) < 2 {
		return fmt.Errorf("header on line %d must have at least 2 columns; got %d", lineNum, len(row))
	}
	c0 := strings.ToLower(stripBOM(row[0]))
	c1 := strings.ToLower(row[1])
	if c0 == "user_id" && c1 == "address" {
		return nil
	}
	if c0 == "address" && c1 == "user_id" {
		return fmt.Errorf("header columns reversed on line %d: expected 'user_id,address'", lineNum)
	}
	return nil // treat unknown header as ok after check
}

func stripBOM(s string) string {
	return strings.TrimPrefix(s, "\uFEFF")
}
