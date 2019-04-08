package main

import (
	"flag"
	"fmt"
	"github.com/google/uuid"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/uasouz/eventhus/examples/bank"
)

func main() {
	flag.Parse()

	commandBus, err := getConfig()
	if err != nil {
		glog.Infoln(err)
		os.Exit(1)
	}

	end := make(chan bool)

	//Create Account
	for i := 0; i < 3; i++ {
		go func() {
			aggregateUUID := uuid.New().String()
			if err != nil {
				return
			}

			//1) Create an account
			var account bank.CreateAccount
			account.AggregateID = aggregateUUID
			account.Owner = "uasouz"

			commandBus.HandleCommand(account)
			fmt.Println("account %s - account created", aggregateUUID)

			fmt.Println("wtf boy")
			//2) Perform a deposit
			time.Sleep(time.Millisecond * 100)
			deposit := bank.PerformDeposit{
				Amount: 300,
			}

			deposit.AggregateID = aggregateUUID
			deposit.Version = 1

			commandBus.HandleCommand(deposit)
			glog.Infof("account %s - deposit performed", aggregateUUID)

			//3) Perform a withdrawl
			time.Sleep(time.Millisecond * 100)
			withdrawl := bank.PerformWithdrawal{
				Amount: 249,
			}

			withdrawl.AggregateID = aggregateUUID
			withdrawl.Version = 2

			commandBus.HandleCommand(withdrawl)
			glog.Infof("account %s - withdrawl performed", aggregateUUID)
		}()
	}
	<-end
}
