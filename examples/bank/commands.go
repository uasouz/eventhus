package bank

import "github.com/uasouz/eventhus"

//CreateAccount assigned to an owner
type CreateAccount struct {
	eventhus.BaseCommand
	Owner string
}

//PerformDeposit to a given account
type PerformDeposit struct {
	eventhus.BaseCommand
	Amount int
}

//ChangeOwner of an account
type ChangeOwner struct {
	eventhus.BaseCommand
	Owner string
}

//PerformWithdrawal to a given account
type PerformWithdrawal struct {
	eventhus.BaseCommand
	Amount int
}
