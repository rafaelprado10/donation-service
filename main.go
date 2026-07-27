package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/joho/godotenv"
)

type Donation struct {
	ID        int       `json:"id"`
	NgoID     int       `json:"ngo_id"`
	Amount    float64   `json:"amount"`
	DonorName string    `json:"donor_name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type App struct {
	DB          *sql.DB
	SqsSvc      *sqs.SQS
	SqsQueueURL string

	ServiceName string
	HostName    string
}

func main() {

	_ = godotenv.Load()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	dbURL := os.Getenv("DATABASE_URL")

	if dbURL == "" {
		dbURL = "host=localhost port=5432 dbname=donation_db user=postgres password=postgres sslmode=disable"
	}

	db, err := sql.Open("pgx", dbURL)

	if err != nil {
		log.Fatalf("Erro ao abrir conexão com banco: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao conectar no PostgreSQL: %v", err)
	}

	defer db.Close()

	log.Println("PostgreSQL conectado com sucesso")

	var sqsSvc *sqs.SQS

	queueURL := os.Getenv("AWS_SQS_URL")
	region := os.Getenv("AWS_REGION")

	if queueURL != "" && region != "" {

		sess, err := session.NewSession(
			&aws.Config{
				Region: aws.String(region),
			},
		)

		if err != nil {
			log.Fatalf("Erro criando sessão AWS: %v", err)
		}

		sqsSvc = sqs.New(sess)

		log.Println("AWS SQS habilitado")
	}

	hostname, err := os.Hostname()

	if err != nil {
		hostname = "unknown-host"
	}

	app := &App{
		DB:          db,
		SqsSvc:      sqsSvc,
		SqsQueueURL: queueURL,

		ServiceName: "donation-service",
		HostName:    hostname,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", app.HealthHandler)
	mux.HandleFunc("/donations", app.DonationHandler)

	log.Printf("donation-service iniciado na porta %s", port)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer stop()

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("donation-service iniciado na porta %s", port)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Erro no servidor HTTP: %v", err)
		}
	}()

	<-ctx.Done()

	log.Println("Encerrando donation-service...")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)

	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Erro ao encerrar servidor: %v", err)
	}

	log.Println("Serviço encerrado")
}

func generateTID() string {

	return fmt.Sprintf(
		"%d",
		time.Now().UnixNano(),
	)

}

func (a *App) Log(
	tid string,
	operation string,
) {

	log.Printf(
		"%s | %s | %s | %s | %s",
		time.Now().
			UTC().
			Format(time.RFC3339Nano),
		a.ServiceName,
		a.HostName,
		tid,
		operation,
	)

}

func (a *App) HealthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(
		map[string]string{
			"status":  "ok",
			"service": "donation-service",
		},
	)
}

func (a *App) DonationHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	start := time.Now()

	tid := generateTID()

	a.Log(
		tid,
		fmt.Sprintf(
			"%s %s started",
			r.Method,
			r.URL.Path,
		),
	)

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	a.logRequest(r, tid)

	switch r.Method {

	case http.MethodPost:

		a.createDonation(w, r, start, tid)

	case http.MethodGet:

		a.listDonations(w, r, start, tid)

	default:

		http.Error(
			w,
			`{"error":"Método não permitido"}`,
			http.StatusMethodNotAllowed,
		)

	}

}

func (a *App) createDonation(
	w http.ResponseWriter,
	r *http.Request,
	start time.Time,
	tid string,
) {

	defer r.Body.Close()

	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		1024*1024,
	)

	body, err := io.ReadAll(r.Body)

	if err != nil {

		http.Error(
			w,
			`{"error":"Payload inválido"}`,
			http.StatusBadRequest,
		)

		return
	}

	var donation Donation

	if err := json.Unmarshal(body, &donation); err != nil {

		http.Error(
			w,
			`{"error":"JSON inválido"}`,
			http.StatusBadRequest,
		)

		return
	}

	if donation.NgoID <= 0 ||
		donation.Amount <= 0 ||
		strings.TrimSpace(donation.DonorName) == "" {

		http.Error(
			w,
			`{"error":"Dados obrigatórios inválidos"}`,
			http.StatusBadRequest,
		)

		return
	}

	donation.Status = "APPROVED"

	a.Log(
		tid,
		fmt.Sprintf(
			"Inserting data into table ngo_id=%d, amount=%.2f, status=%s",
			donation.NgoID,
			donation.Amount,
			donation.Status,
		),
	)

	err = a.DB.QueryRowContext(
		r.Context(),
		`
		INSERT INTO donations
		(
			ngo_id,
			amount,
			donor_name,
			status
		)
		VALUES
		($1,$2,$3,$4)
		RETURNING id,created_at
		`,
		donation.NgoID,
		donation.Amount,
		donation.DonorName,
		donation.Status,
	).Scan(
		&donation.ID,
		&donation.CreatedAt,
	)

	if err != nil {

		a.Log(
			tid,
			fmt.Sprintf(
				"Error to insert donation id=%d: %v",
				donation.ID,
				err,
			),
		)

		http.Error(
			w,
			`{"error":"Erro interno"}`,
			http.StatusInternalServerError,
		)

		return
	}

	if a.SqsSvc != nil {

		err := a.sendNotificationEvent(donation, tid)

		if err != nil {

			a.Log(
				tid,
				fmt.Sprintf(
					"Error to send notification for donation id=%d: %v",
					donation.ID,
					err,
				),
			)
		}
	}

	w.WriteHeader(
		http.StatusCreated,
	)

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	if err := json.NewEncoder(w).Encode(donation); err != nil {
		log.Printf("erro ao responder JSON: %v", err)
	}

	a.Log(
		tid,
		fmt.Sprintf(
			"donation.created id=%d duration_ms=%d",
			donation.ID,
			time.Since(start).Milliseconds(),
		),
	)

}

func (a *App) listDonations(
	w http.ResponseWriter,
	r *http.Request,
	start time.Time,
	tid string,
) {

	rows, err := a.DB.QueryContext(
		r.Context(),
		`
		SELECT
			id,
			ngo_id,
			amount,
			donor_name,
			status,
			created_at
		FROM donations
		ORDER BY id DESC
		`,
	)

	if err != nil {

		http.Error(
			w,
			`{"error":"Erro interno"}`,
			http.StatusInternalServerError,
		)

		return
	}

	defer rows.Close()

	donations := []Donation{}

	for rows.Next() {

		var d Donation

		err := rows.Scan(
			&d.ID,
			&d.NgoID,
			&d.Amount,
			&d.DonorName,
			&d.Status,
			&d.CreatedAt,
		)

		if err != nil {

			http.Error(
				w,
				`{"error":"Erro interno"}`,
				http.StatusInternalServerError,
			)

			return
		}

		donations = append(
			donations,
			d,
		)

	}

	if err := rows.Err(); err != nil {

		http.Error(
			w,
			`{"error":"Erro interno"}`,
			http.StatusInternalServerError,
		)

		return
	}

	json.NewEncoder(w).Encode(donations)

	a.Log(
		tid,
		fmt.Sprintf(
			"donations.list count=%d duration_ms=%d",
			len(donations),
			time.Since(start).Milliseconds(),
		),
	)

}

func (a *App) sendNotificationEvent(
	donation Donation,
	tid string,
) error {

	body, err := json.Marshal(donation)

	if err != nil {
		return err
	}

	_, err = a.SqsSvc.SendMessage(
		&sqs.SendMessageInput{

			MessageBody: aws.String(
				string(body),
			),

			QueueUrl: aws.String(
				a.SqsQueueURL,
			),
		},
	)

	return err
}

func (a *App) logRequest(
	r *http.Request,
	tid string,
) {

	a.Log(
		tid,
		fmt.Sprintf(
			"request method=%s path=%s remote=%s",
			r.Method,
			r.URL.Path,
			r.RemoteAddr,
		),
	)

}
