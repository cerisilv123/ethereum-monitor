package models

type TxMatchedEvent struct {
	UserID      string `json:"userId"`
	From        string `json:"from"`
	To          string `json:"to,omitempty"`
	Amount      string `json:"amount"` // in wei
	Hash        string `json:"hash"`
	BlockNumber uint64 `json:"blockNumber"`
}
