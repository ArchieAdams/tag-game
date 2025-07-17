package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Global DynamoDB service client and table name, initialized once
var (
	svc              *dynamodb.DynamoDB
	gamesTableName   string
	playersTableName string
)

type Game struct {
	GameId         string `json:"gameId" dynamodbav:"gameId"`
	GameName       string `json:"gameName" dynamodbav:"gameName"`
	HasGameStarted bool   `json:"hasGameStarted" dynamodbav:"hasGameStarted"`
	OwnerId        string `json:"ownerId" dynamodbav:"ownerId"`
}

type Player struct {
	PlayerId   string `json:"playerId" dynamodbav:"playerId"`
	PlayerName string `json:"playerName" dynamodbav:"playerName"`
	GameId     string `json:"gameId" dynamodbav:"gameId"`
}

type CreateGameRequest struct {
	GameName   string `json:"gameName"`
	PlayerName string `json:"playerName"`
	GameRequest
}

type JoinGameRequest struct {
	GameRequest
	PlayerName string `json:"playerName"`
}

type GameRequest struct {
	GameId   string `json:"gameId"`
	PlayerId string `json:"playerId"`
}

type RemovePlayerRequest struct {
	GameRequest
	PlayerIdToRemove string `json:"playerIdToRemove"`
}

func init() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc = dynamodb.New(sess)

	gamesTableName = os.Getenv("GAMES_TABLE_NAME")
	playersTableName = os.Getenv("PLAYERS_TABLE_NAME")

	if gamesTableName == "" || playersTableName == "" {
		log.Fatal("Environment variables GAMES_TABLE_NAME or PLAYERS_TABLE_NAME are not set.")
	}
}

func CreateGameAndPlayer(gameRequest CreateGameRequest) error {
	gameItem := Game{
		GameId:         gameRequest.GameId,
		GameName:       gameRequest.GameName,
		HasGameStarted: false,
		OwnerId:        gameRequest.PlayerId,
	}

	playerItem := Player{
		PlayerId:   gameRequest.PlayerId,
		PlayerName: gameRequest.PlayerName,
		GameId:     gameRequest.GameId,
	}

	gameAV, err := dynamodbattribute.MarshalMap(gameItem)
	if err != nil {
		return fmt.Errorf("failed to marshal game item: %w", err)
	}

	playerAV, err := dynamodbattribute.MarshalMap(playerItem)
	if err != nil {
		return fmt.Errorf("failed to marshal player item: %w", err)
	}

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Put: &dynamodb.Put{
					TableName:           aws.String(gamesTableName),
					Item:                gameAV,
					ConditionExpression: aws.String("attribute_not_exists(gameId)"),
				},
			},
			{
				Put: &dynamodb.Put{
					TableName:           aws.String(playersTableName),
					Item:                playerAV,
					ConditionExpression: aws.String("attribute_not_exists(playerId)"),
				},
			},
		},
	}

	_, err = svc.TransactWriteItems(input)
	if err != nil {
		return fmt.Errorf("transaction failed: %w", err)
	}

	fmt.Printf("Successfully created game and player.\n")
	return nil
}

func CreatePlayer(gameRequest JoinGameRequest) error {

	itemToWrite := Player{
		PlayerId:   gameRequest.PlayerId,
		PlayerName: gameRequest.PlayerName,
		GameId:     gameRequest.GameId,
	}

	av, err := dynamodbattribute.MarshalMap(itemToWrite)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	putInput := &dynamodb.PutItemInput{
		TableName:           aws.String(playersTableName),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(playerId)"),
	}

	_, err = svc.PutItem(putInput)
	if err != nil {
		return fmt.Errorf("failed to put item into DynamoDB: %w", err)
	}

	fmt.Printf("Successfully wrote player with ID: %s\n", gameRequest.PlayerId)
	return nil
}

func JoinGame(gameRequest JoinGameRequest) error {

	input := &dynamodb.GetItemInput{
		TableName: aws.String(gamesTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"gameId": {
				S: aws.String(gameRequest.GameId),
			},
		},
	}

	result, err := svc.GetItem(input)
	if err != nil {
		return fmt.Errorf("failed to query DynamoDB: %w", err)
	}

	if result.Item == nil {
		return fmt.Errorf("game %s not found", gameRequest.GameId)
	}

	return nil
}

func IsGameOwner(gameId string, playerId string) (bool, error) {
	getInput := &dynamodb.GetItemInput{
		TableName: aws.String(gamesTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"gameId": {
				S: aws.String(gameId),
			},
		},
	}

	result, err := svc.GetItem(getInput)
	if err != nil {
		return false, fmt.Errorf("failed to get game: %w", err)
	}

	if result.Item == nil {
		return false, fmt.Errorf("game with ID %s not found", gameId)
	}

	var game Game
	if err := dynamodbattribute.UnmarshalMap(result.Item, &game); err != nil {
		return false, fmt.Errorf("failed to unmarshal game: %w", err)
	}

	return game.OwnerId == playerId, nil
}

func DeleteGame(gameRequest GameRequest) error {
	isOwner, err := IsGameOwner(gameRequest.GameId, gameRequest.PlayerId)
	if err != nil {
		return err
	}
	if !isOwner {
		return fmt.Errorf("unauthorized: only the game owner can delete this game")
	}

	// Delete the game item
	_, err = svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(gamesTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"gameId": {
				S: aws.String(gameRequest.GameId),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete game: %w", err)
	}
	log.Printf("Deleted game: %s", gameRequest.GameId)

	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(playersTableName),
		IndexName:              aws.String("gameIdIndex"),
		KeyConditionExpression: aws.String("gameId = :gid"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":gid": {S: aws.String(gameRequest.GameId)},
		},
	}

	queryResult, err := svc.Query(queryInput)
	if err != nil {
		return fmt.Errorf("failed to query players by gameId: %w", err)
	}

	// Delete all associated players
	for _, item := range queryResult.Items {
		var player Player
		if err := dynamodbattribute.UnmarshalMap(item, &player); err != nil {
			log.Printf("Failed to unmarshal player: %v", err)
			continue
		}

		_, err := svc.DeleteItem(&dynamodb.DeleteItemInput{
			TableName: aws.String(playersTableName),
			Key: map[string]*dynamodb.AttributeValue{
				"playerId": {S: aws.String(player.PlayerId)},
			},
		})
		if err != nil {
			log.Printf("Failed to delete player %s: %v", player.PlayerId, err)
		} else {
			log.Printf("Deleted player: %s", player.PlayerId)
		}
	}

	return nil
}

func RemovePlayer(removePlayerRequest RemovePlayerRequest) error {
	isOwner, err := IsGameOwner(removePlayerRequest.GameId, removePlayerRequest.PlayerId)
	if err != nil {
		return err
	}
	if !isOwner {
		return fmt.Errorf("unauthorized: only the game owner can remove a player")
	}

	// Delete the game item
	_, err = svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(playersTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"playerId": {
				S: aws.String(removePlayerRequest.PlayerIdToRemove),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to remove player: %w", err)
	}
	log.Printf("Removed player: %s", removePlayerRequest.GameId)

	return nil
}

func StartGame(gameRequest GameRequest) error {
	isOwner, err := IsGameOwner(gameRequest.GameId, gameRequest.PlayerId)
	if err != nil {
		return err
	}
	if !isOwner {
		return fmt.Errorf("unauthorized: only the game owner can start this game")
	}

	err = SetGameState(true, gameRequest.GameId)
	if err != nil {
		return err
	}

	return nil
}

func EndGame(gameRequest GameRequest) error {
	isOwner, err := IsGameOwner(gameRequest.GameId, gameRequest.PlayerId)
	if err != nil {
		return err
	}
	if !isOwner {
		return fmt.Errorf("unauthorized: only the game owner can end this game")
	}

	err = SetGameState(false, gameRequest.GameId)
	if err != nil {
		return err
	}

	return nil
}

func SetGameState(state bool, gameId string) error {
	_, err := svc.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(gamesTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"gameId": {
				S: aws.String(gameId),
			},
		},
		UpdateExpression: aws.String("SET hasGameStarted = :state"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":state": {
				BOOL: aws.Bool(state),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update game state: %w", err)
	}
	return nil
}

func PlayerList(gameRequest GameRequest) ([]Player, error) {
	isOwner, err := IsGameOwner(gameRequest.GameId, gameRequest.PlayerId)
	if err != nil {
		return nil, err
	}
	if !isOwner {
		return nil, fmt.Errorf("unauthorized: only the game owner can end this game")
	}

	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(playersTableName),
		IndexName:              aws.String("gameIdIndex"),
		KeyConditionExpression: aws.String("gameId = :gid"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":gid": {S: aws.String(gameRequest.GameId)},
		},
	}

	queryResult, err := svc.Query(queryInput)
	if err != nil {
		return nil, fmt.Errorf("failed to query players by gameId: %w", err)
	}

	var playerList []Player
	for _, item := range queryResult.Items {
		var player Player
		if err := dynamodbattribute.UnmarshalMap(item, &player); err != nil {
			log.Printf("Failed to unmarshal player: %v", err)
			continue
		}
		playerList = append(playerList, player)
	}

	return playerList, nil
}

func LeaveGame(gameRequest GameRequest) error {
	_, err := svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(playersTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"playerId": {
				S: aws.String(gameRequest.PlayerId),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to remove player: %w", err)
	}
	log.Printf("Removed Player: %s", gameRequest.PlayerId)

	return nil
}

func GameState(gameRequest GameRequest) (bool, error) {
	getInput := &dynamodb.GetItemInput{
		TableName: aws.String(gamesTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"gameId": {
				S: aws.String(gameRequest.GameId),
			},
		},
	}

	result, err := svc.GetItem(getInput)
	if err != nil {
		return false, fmt.Errorf("failed to get game: %w", err)
	}

	if result.Item == nil {
		return false, fmt.Errorf("game with ID %s not found", gameRequest.GameId)
	}

	var game Game
	if err := dynamodbattribute.UnmarshalMap(result.Item, &game); err != nil {
		return false, fmt.Errorf("failed to unmarshal game: %w", err)
	}

	return game.HasGameStarted, nil
}

func HandleRequest(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	switch request.Path {
	case "/createGame":
		return handleCreateGame(request)
	case "/joinGame":
		return handleJoinGame(request)
	case "/deleteGame":
		return handleDeleteGame(request)
	case "/removePlayer":
		return handleRemovePlayer(request)
	case "/startGame":
		return handleStartGame(request)
	case "/endGame":
		return handleEndGame(request)
	case "/playerList":
		return handlePlayerList(request)
	case "/leaveGame":
		return handleLeaveGame(request)
	case "/gameState":
		return handleGameState(request)
	case "/isOwner":
		return handleIsOwner(request)
	default:
		return events.APIGatewayProxyResponse{
			StatusCode: 404,
			Body:       "Route not found",
		}, nil
	}
}

func handleCreateGame(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body CreateGameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Invalid game request"}, nil
	}
	err = CreateGameAndPlayer(body)
	if err != nil {
		var awsErr *dynamodb.TransactionCanceledException
		if errors.As(err, &awsErr) {
			return events.APIGatewayProxyResponse{
				StatusCode: 409,
				Body:       "Game already exists or player already joined",
			}, nil
		}

		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf("Create failed: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf(`{"message": "%s has been made by %s"}`, body.GameName, body.PlayerName),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleJoinGame(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body JoinGameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid JSON in request body",
		}, nil
	}

	err = JoinGame(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 404,
			Body:       fmt.Sprintf("Game not found: %v", err),
		}, nil
	}

	// Try to create player, but avoid duplicates
	err = CreatePlayer(body)
	if err != nil {
		var awsErr awserr.Error
		if errors.As(err, &awsErr) && awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return events.APIGatewayProxyResponse{
				StatusCode: 409,
				Body:       "Player already exists in this game",
			}, nil
		}

		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf("Failed to create player: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf(`{"message": "Player %s joined game %s"}`, body.PlayerName, body.GameId),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleDeleteGame(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body GameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid delete game request",
		}, nil
	}

	err = DeleteGame(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("Delete failed: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf("Game %s and associated players deleted successfully", body.GameId),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleRemovePlayer(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body RemovePlayerRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid player remove request",
		}, nil
	}

	err = RemovePlayer(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("Remove failed: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf("Player reomved %s and associated players deleted successfully", body.PlayerIdToRemove),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleStartGame(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body GameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid start game request",
		}, nil
	}
	err = StartGame(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("Start Game failed: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf("Game started: %s", body.GameId),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleEndGame(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body GameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid end game request",
		}, nil
	}
	err = EndGame(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("End Game failed: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf("Game ended: %s", body.GameId),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handlePlayerList(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body GameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid player list request",
		}, nil
	}
	playerList, err := PlayerList(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("Player List failed: %v", err),
		}, nil
	}

	playersJSON, err := json.Marshal(playerList)
	if err != nil {
		log.Fatalf("Failed to marshal players: %v", err)
	}
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(playersJSON),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleLeaveGame(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body GameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid leave game request",
		}, nil
	}

	err = LeaveGame(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("Leave Game failed: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf("Player Left: %s", body.PlayerId),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleGameState(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body GameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid game state request",
		}, nil
	}

	gameState, err := GameState(body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("Game state failed: %v", err),
		}, nil
	}

	response, err := json.Marshal(map[string]bool{
		"gameState": gameState,
	})
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Failed to marshal response",
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(response),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func handleIsOwner(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var body GameRequest
	err := json.Unmarshal([]byte(request.Body), &body)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       "Invalid is owner request",
		}, nil
	}

	isOwner, err := IsGameOwner(body.GameId, body.PlayerId)
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 400,
			Body:       fmt.Sprintf("Is Owner failed: %v", err),
		}, nil
	}

	response, err := json.Marshal(map[string]bool{
		"isOwner": isOwner,
	})
	if err != nil {
		return events.APIGatewayProxyResponse{
			StatusCode: 500,
			Body:       "Failed to marshal response",
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       string(response),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
