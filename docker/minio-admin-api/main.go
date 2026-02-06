package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/minio/madmin-go/v3"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	docs "minio-admin-api/docs"
)

// --- 1. CONFIGURATION & HELPERS ---

var JWT_SECRET = []byte(getEnv("JWT_SECRET", "super_secret_key_change_me"))

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getMinioAdminClient() (*madmin.AdminClient, error) {
	endpoint := getEnv("MINIO_ENDPOINT", "minio:9000")
	accessKey := getEnv("MINIO_ROOT_USER", "minioadmin")
	secretKey := getEnv("MINIO_ROOT_PASS", "minioadmin")
	return madmin.New(endpoint, accessKey, secretKey, false)
}

func generateToken(username string) (string, error) {
	claims := jwt.MapClaims{
		"username": username,
		"exp":      time.Now().Add(time.Hour * 24).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(JWT_SECRET)
}

// --- 2. STRUCTS (REQUEST MODELS) ---

type LoginRequest struct {
	Username string `json:"username" binding:"required" example:"minioadmin"`
	Password string `json:"password" binding:"required" example:"minioadmin"`
}

type CreateUserRequest struct {
	AccessKey string `json:"accessKey" binding:"required" example:"new_user"`
	SecretKey string `json:"secretKey" binding:"required" example:"StrongPass123!"`
}

type CreatePolicyRequest struct {
	PolicyName    string                 `json:"policyName" binding:"required" example:"my-bucket-policy"`
	PolicyContent map[string]interface{} `json:"policyContent" binding:"required" swaggertype:"object" example:"{\"Version\": \"2012-10-17\", \"Statement\": [{\"Effect\": \"Allow\", \"Action\": [\"s3:*\"], \"Resource\": [\"arn:aws:s3:::my-bucket-name\", \"arn:aws:s3:::my-bucket-name/*\"]}]}"`
}

type AssignPolicyRequest struct {
	Username   string `json:"username" binding:"required" example:"new_user"`
	PolicyName string `json:"policyName" binding:"required" example:"readwrite"`
}

type RevokePolicyRequest struct {
	Username   string `json:"username" binding:"required" example:"new_user"`
	PolicyName string `json:"policyName" binding:"required" example:"readwrite"`
}

// --- 3. MIDDLEWARES ---

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

func AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Authorization header is missing"})
			c.Abort()
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid authorization format. Use 'Bearer <token>'"})
			c.Abort()
			return
		}

		tokenString := parts[1]
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method")
			}
			return JWT_SECRET, nil
		})

		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid or expired token"})
			c.Abort()
			return
		}

		c.Next()
	}
}

// --- 4. MAIN FUNCTION ---

// @title MinIO Admin API
// @version 6.0
// @description MinIO Management API (Full CRUD)
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
func main() {
	gin.SetMode(getEnv("GIN_MODE", "debug"))

	swaggerHost := getEnv("SWAGGER_HOST", "localhost:6061")
	docs.SwaggerInfo.Host = swaggerHost
	docs.SwaggerInfo.BasePath = "/api/v1"

	r := gin.Default()
	r.Use(CORSMiddleware())

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	v1 := r.Group("/api/v1")
	{
		v1.POST("/login", LoginHandler)

		protected := v1.Group("/")
		protected.Use(AuthMiddleware())
		{
			// === USER ROUTES ===
			users := protected.Group("/users")
			{
				users.GET("", ListUsersHandler)
				users.POST("", CreateUserHandler)
				users.GET("/:username/policy", GetUserPolicyHandler)
				users.DELETE("/:username", DeleteUserHandler)
			}

			// === POLICY ROUTES ===
			policies := protected.Group("/policies")
			{
				policies.GET("", ListPoliciesHandler)
				policies.POST("", CreatePolicyHandler)
				policies.DELETE("/:policyName", DeletePolicyHandler)

				policies.POST("/assign", AssignPolicyHandler)
				policies.POST("/revoke", RevokePolicyHandler)
			}
		}
	}

	log.Printf("Server starting on port 8080. Swagger Host: %s", swaggerHost)
	r.Run(":8080")
}

// --- 5. HANDLERS ---

// === AUTH ===

// @Summary Login to MinIO
// @Tags Authentication
// @Accept json
// @Produce json
// @Param request body LoginRequest true "Credentials"
// @Success 200 {object} map[string]string
// @Router /login [post]
func LoginHandler(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	endpoint := getEnv("MINIO_ENDPOINT", "minio:9000")
	client, err := madmin.New(endpoint, req.Username, req.Password, false)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid configuration"})
		return
	}
	_, err = client.ServerInfo(context.Background())
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication failed"})
		return
	}
	token, err := generateToken(req.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Token generation failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Login successful", "token": token})
}

// === USER HANDLERS ===

// @Summary List all Users
// @Tags User Management
// @Security BearerAuth
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /users [get]
func ListUsersHandler(c *gin.Context) {
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	usersMap, err := mdmClnt.ListUsers(context.Background())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	type UserResponse struct {
		AccessKey string `json:"accessKey"`
		Status    string `json:"status"`
		Policy    string `json:"policy,omitempty"`
	}
	var userList []UserResponse
	for k, v := range usersMap {
		userList = append(userList, UserResponse{AccessKey: k, Status: string(v.Status), Policy: v.PolicyName})
	}
	sort.Slice(userList, func(i, j int) bool { return userList[i].AccessKey < userList[j].AccessKey })
	c.JSON(http.StatusOK, gin.H{"count": len(userList), "users": userList})
}

// @Summary Create a new User
// @Tags User Management
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body CreateUserRequest true "User Info"
// @Router /users [post]
func CreateUserHandler(c *gin.Context) {
	var req CreateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	err = mdmClnt.AddUser(context.Background(), req.AccessKey, req.SecretKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "User created", "user": req.AccessKey})
}

// @Summary Delete a User
// @Description Permanently remove a user from MinIO
// @Tags User Management
// @Security BearerAuth
// @Param username path string true "Username to delete"
// @Success 200 {object} map[string]string
// @Router /users/{username} [delete]
func DeleteUserHandler(c *gin.Context) {
	username := c.Param("username")
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	err = mdmClnt.RemoveUser(context.Background(), username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not delete user: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "User deleted successfully", "user": username})
}

// @Summary Get User Policy
// @Tags User Management
// @Security BearerAuth
// @Produce json
// @Param username path string true "Username"
// @Router /users/{username}/policy [get]
func GetUserPolicyHandler(c *gin.Context) {
	username := c.Param("username")
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	info, err := mdmClnt.GetUserInfo(context.Background(), username)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"username": username, "status": info.Status, "policyName": info.PolicyName})
}

// === POLICY HANDLERS ===

// @Summary List Policies
// @Tags Policy Management
// @Security BearerAuth
// @Produce json
// @Router /policies [get]
func ListPoliciesHandler(c *gin.Context) {
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	policies, err := mdmClnt.ListCannedPolicies(context.Background())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	policyNames := make([]string, 0, len(policies))
	for name := range policies {
		policyNames = append(policyNames, name)
	}
	sort.Strings(policyNames)
	c.JSON(http.StatusOK, gin.H{"count": len(policyNames), "policies": policyNames})
}

// @Summary Create Policy
// @Tags Policy Management
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body CreatePolicyRequest true "Policy Content"
// @Router /policies [post]
func CreatePolicyHandler(c *gin.Context) {
	var req CreatePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	policyBytes, err := json.Marshal(req.PolicyContent)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON format"})
		return
	}
	err = mdmClnt.AddCannedPolicy(context.Background(), req.PolicyName, policyBytes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Policy created", "name": req.PolicyName})
}

// @Summary Delete a Policy
// @Description Remove a custom policy (Cannot remove built-in policies like readwrite)
// @Tags Policy Management
// @Security BearerAuth
// @Param policyName path string true "Policy Name to delete"
// @Success 200 {object} map[string]string
// @Router /policies/{policyName} [delete]
func DeletePolicyHandler(c *gin.Context) {
	policyName := c.Param("policyName")
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	err = mdmClnt.RemoveCannedPolicy(context.Background(), policyName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not delete policy (ensure it is not in use): " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Policy deleted successfully", "policy": policyName})
}

// @Summary Assign Policy to User
// @Tags Policy Management
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body AssignPolicyRequest true "Assignment"
// @Router /policies/assign [post]
func AssignPolicyHandler(c *gin.Context) {
	var req AssignPolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	err = mdmClnt.SetPolicy(context.Background(), req.PolicyName, req.Username, false)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Policy attached", "user": req.Username, "policy": req.PolicyName})
}

// @Summary Revoke Policy from User
// @Description Unset/Detach a specific policy from a user
// @Tags Policy Management
// @Security BearerAuth
// @Accept json
// @Produce json
// @Param request body RevokePolicyRequest true "Revoke Details"
// @Success 200 {object} map[string]string
// @Router /policies/revoke [post]
func RevokePolicyHandler(c *gin.Context) {
	var req RevokePolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	mdmClnt, err := getMinioAdminClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 1. Get current user info to find existing policies
	userInfo, err := mdmClnt.GetUserInfo(context.Background(), req.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not fetch user info: " + err.Error()})
		return
	}

	// 2. Filter out the policy to be revoked
	currentPolicies := strings.Split(userInfo.PolicyName, ",")
	var newPolicies []string
	found := false
	for _, p := range currentPolicies {
		p = strings.TrimSpace(p)
		if p == req.PolicyName {
			found = true
			continue // Skip this policy
		}
		if p != "" {
			newPolicies = append(newPolicies, p)
		}
	}

	if !found {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Policy not assigned to user"})
		return
	}

	// 3. Update the user with the new policy list
	newPolicyStr := strings.Join(newPolicies, ",")
	err = mdmClnt.SetPolicy(context.Background(), newPolicyStr, req.Username, false)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not update policies: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Policy revoked successfully", "user": req.Username, "remaining_policies": newPolicyStr})
}