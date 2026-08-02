package admin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	mw "github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/messagebus"
	"github.com/pablojhp.pergo/internal/platform/queue"
	"github.com/pablojhp.pergo/internal/repository"
	"github.com/pablojhp.pergo/templates/pages"
)

const (
	maxCampaignBatchSize                = 1000
	maxCampaignCSVBytes           int64 = 5 << 20
	maxCampaignUploadBodyBytes    int64 = 6 << 20
	maxCampaignCreateBodyBytes    int64 = 16 << 20
	maxCampaignCSVRows                  = 10000
	maxCampaignCSVColumns               = 32
	maxCampaignCSVFieldBytes            = 4096
	maxCampaignCSVHeaderBytes           = maxCampaignCSVColumns * (maxCampaignCSVFieldBytes + 1)
	maxCampaignVariables                = maxCampaignCSVColumns * 2
	maxCampaignVariableKeyBytes         = 128
	maxCampaignVariableValueBytes       = maxCampaignCSVFieldBytes
	maxCampaignVariablesBytes           = maxCampaignCSVHeaderBytes * 2
	maxCampaignSkippedRawBytes          = maxCampaignCSVHeaderBytes
	maxCampaignSkippedReasonBytes       = 512
)

var (
	errCampaignCSVTooLarge     = errors.New("campaign CSV is too large")
	errCampaignCSVTooManyRows  = errors.New("campaign CSV has too many rows")
	errCampaignCSVInvalidShape = errors.New("campaign CSV has invalid shape")
	errCampaignBatchTooLarge   = errors.New("one campaign recipient exceeds the queue payload limit")

	wabaTemplatePlaceholderPattern = regexp.MustCompile(`\{\{([0-9]+)\}\}`)
	unresolvedTemplateValuePattern = regexp.MustCompile(`\{\{[^{}]+\}\}`)
	wabaTemplateLanguagePattern    = regexp.MustCompile(`^[A-Za-z]{2,3}(?:[_-][A-Za-z]{2,4})?$`)
)

type campaignWABATemplateComponent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type campaignWABATemplateSnapshot struct {
	Language           string
	BodyParameterCount int
}

type CampaignHandler struct {
	CampaignRepo   *repository.CampaignRepository
	TemplateRepo   *repository.WABATemplateRepository
	ConnectionRepo *repository.ConnectionRepository
	Publisher      *queue.JetStreamPublisher
}

func NewCampaignHandler(
	campaignRepo *repository.CampaignRepository,
	templateRepo *repository.WABATemplateRepository,
	connectionRepo *repository.ConnectionRepository,
	publisher *queue.JetStreamPublisher,
) *CampaignHandler {
	return &CampaignHandler{
		CampaignRepo:   campaignRepo,
		TemplateRepo:   templateRepo,
		ConnectionRepo: connectionRepo,
		Publisher:      publisher,
	}
}

func (h *CampaignHandler) List(c *echo.Context) error {
	workspaceIDStr, err := echo.PathParam[string](c, "workspace_id")
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}
	workspaceID, err := uuid.Parse(workspaceIDStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}

	campaigns, err := h.CampaignRepo.ListByWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		return c.String(http.StatusInternalServerError, "failed to list campaigns")
	}

	templates, err := h.TemplateRepo.ListByWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		templates = []repository.WABATemplate{}
	}

	connections, err := h.ConnectionRepo.ListByWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		connections = []*repository.Connection{}
	}

	if mw.IsHTMX(c) {
		return mw.Render(c, http.StatusOK, pages.CampaignsContent(workspaceID, campaigns, templates, connections))
	}
	return mw.Render(c, http.StatusOK, pages.CampaignsPage(workspaceID, campaigns, templates, connections))
}

func (h *CampaignHandler) NewForm(c *echo.Context) error {
	workspaceIDStr, err := echo.PathParam[string](c, "workspace_id")
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}
	workspaceID, err := uuid.Parse(workspaceIDStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}

	templates, err := h.TemplateRepo.ListByWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		templates = []repository.WABATemplate{}
	}

	connections, err := h.ConnectionRepo.ListByWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		connections = []*repository.Connection{}
	}

	return mw.Render(c, http.StatusOK, pages.CampaignCreateForm(workspaceID, templates, connections))
}

func (h *CampaignHandler) UploadCSV(c *echo.Context) error {
	c.Request().Body = http.MaxBytesReader(
		c.Response(),
		c.Request().Body,
		maxCampaignUploadBodyBytes,
	)
	fileHeader, err := c.FormFile("csv_file")
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return c.String(http.StatusRequestEntityTooLarge, "uploaded file exceeds the maximum size")
		}
		return c.String(http.StatusBadRequest, "failed to read uploaded file")
	}
	if c.Request().MultipartForm != nil {
		defer func() { _ = c.Request().MultipartForm.RemoveAll() }()
	}
	if fileHeader.Size > maxCampaignCSVBytes {
		return c.String(http.StatusRequestEntityTooLarge, "uploaded file exceeds the maximum size")
	}

	src, err := fileHeader.Open()
	if err != nil {
		return c.String(http.StatusBadRequest, "failed to open uploaded file")
	}
	defer func() { _ = src.Close() }()

	preview, err := parseCampaignCSV(src)
	if err != nil {
		switch {
		case errors.Is(err, errCampaignCSVTooLarge):
			return c.String(http.StatusRequestEntityTooLarge, "uploaded file exceeds the maximum size")
		case errors.Is(err, errCampaignCSVTooManyRows):
			return c.String(http.StatusBadRequest, fmt.Sprintf("CSV exceeds the maximum of %d rows", maxCampaignCSVRows))
		default:
			return c.String(http.StatusBadRequest, fmt.Sprintf("failed to parse CSV: %v", err))
		}
	}

	return mw.Render(
		c,
		http.StatusOK,
		pages.CSVPreviewSegment(
			preview.Summary,
			preview.RawHeaders,
			preview.SampleRows,
			preview.Recipients,
			preview.Skipped,
		),
	)
}

type campaignCSVPreview struct {
	Summary    map[string]int
	RawHeaders []string
	SampleRows [][]string
	Recipients []domain.CampaignRecipient
	Skipped    []domain.SkippedRow
}

func parseCampaignCSV(src io.Reader) (*campaignCSVPreview, error) {
	limited := &io.LimitedReader{R: src, N: maxCampaignCSVBytes + 1}
	buffered := bufio.NewReaderSize(limited, maxCampaignCSVHeaderBytes+1)
	headerLine, err := buffered.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, fmt.Errorf("%w: header is too large", errCampaignCSVInvalidShape)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(headerLine) == 0 {
		return nil, fmt.Errorf("%w: uploaded file is empty", errCampaignCSVInvalidShape)
	}
	if limited.N == 0 {
		return nil, errCampaignCSVTooLarge
	}

	headerCopy := append([]byte(nil), headerLine...)
	delimiter := domain.SniffDelimiter(string(headerCopy))
	reader := csv.NewReader(io.MultiReader(bytes.NewReader(headerCopy), buffered))
	reader.Comma = delimiter
	reader.LazyQuotes = true

	rawHeaders, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errCampaignCSVInvalidShape, err)
	}
	if err := validateCampaignCSVRecord(rawHeaders); err != nil {
		return nil, err
	}
	if len(rawHeaders) == 0 {
		return nil, fmt.Errorf("%w: CSV contains no headers", errCampaignCSVInvalidShape)
	}

	headers := make([]string, len(rawHeaders))
	for i, header := range rawHeaders {
		if len(header) > maxCampaignVariableKeyBytes {
			return nil, fmt.Errorf(
				"%w: header exceeds %d bytes",
				errCampaignCSVInvalidShape,
				maxCampaignVariableKeyBytes,
			)
		}
		headers[i] = strings.TrimSpace(strings.ToLower(header))
		if headers[i] == "" {
			headers[i] = "column_" + strconv.Itoa(i)
		}
	}

	phoneColIdx := 0
	phoneKeywords := []string{"phone", "telefone", "fone", "tel", "to", "number", "numero", "celular", "contato", "contact"}
	foundPhone := false
	for i, header := range headers {
		for _, keyword := range phoneKeywords {
			if strings.Contains(header, keyword) {
				phoneColIdx = i
				foundPhone = true
				break
			}
		}
		if foundPhone {
			break
		}
	}

	preview := &campaignCSVPreview{
		RawHeaders: append([]string(nil), rawHeaders...),
		Recipients: make([]domain.CampaignRecipient, 0),
		Skipped:    make([]domain.SkippedRow, 0),
		SampleRows: make([][]string, 0, 5),
	}
	seen := make(map[string]struct{})
	totalRows := 0
	validCount := 0
	duplicateCount := 0
	invalidCount := 0

	for {
		row, readErr := reader.Read()
		if limited.N == 0 {
			return nil, errCampaignCSVTooLarge
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("%w: %v", errCampaignCSVInvalidShape, readErr)
		}
		totalRows++
		if totalRows > maxCampaignCSVRows {
			return nil, errCampaignCSVTooManyRows
		}
		if err := validateCampaignCSVRecord(row); err != nil {
			return nil, err
		}
		if campaignCSVRowEmpty(row) {
			continue
		}

		rawInput := strings.Join(row, string(delimiter))
		lineNumber := totalRows + 1
		cleanPhone, valid := domain.SanitizePhone(row[phoneColIdx])
		if !valid {
			invalidCount++
			preview.Skipped = append(preview.Skipped, domain.SkippedRow{
				LineNumber: lineNumber,
				RawInput:   rawInput,
				Reason:     fmt.Sprintf("numero de telefone invalido (tamanho %d)", len(cleanPhone)),
			})
			continue
		}
		if _, exists := seen[cleanPhone]; exists {
			duplicateCount++
			preview.Skipped = append(preview.Skipped, domain.SkippedRow{
				LineNumber: lineNumber,
				RawInput:   rawInput,
				Reason:     "numero de telefone duplicado",
			})
			continue
		}
		seen[cleanPhone] = struct{}{}
		validCount++

		variables := make(map[string]string, len(row)*2)
		for columnIndex, columnValue := range row {
			variables[headers[columnIndex]] = columnValue
			variables[strconv.Itoa(columnIndex)] = columnValue
		}
		preview.Recipients = append(preview.Recipients, domain.CampaignRecipient{
			To:        cleanPhone,
			Variables: variables,
		})
		if len(preview.SampleRows) < 5 {
			preview.SampleRows = append(preview.SampleRows, append([]string(nil), row...))
		}
	}

	if totalRows == 0 {
		return nil, fmt.Errorf("%w: CSV contains no data", errCampaignCSVInvalidShape)
	}
	preview.Summary = map[string]int{
		"total":     totalRows,
		"valid":     validCount,
		"duplicate": duplicateCount,
		"invalid":   invalidCount,
	}
	return preview, nil
}

func validateCampaignCSVRecord(record []string) error {
	if len(record) == 0 || len(record) > maxCampaignCSVColumns {
		return fmt.Errorf(
			"%w: each row must contain between 1 and %d columns",
			errCampaignCSVInvalidShape,
			maxCampaignCSVColumns,
		)
	}
	for _, field := range record {
		if len(field) > maxCampaignCSVFieldBytes {
			return fmt.Errorf(
				"%w: field exceeds %d bytes",
				errCampaignCSVInvalidShape,
				maxCampaignCSVFieldBytes,
			)
		}
	}
	return nil
}

func campaignCSVRowEmpty(row []string) bool {
	for _, field := range row {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}

func decodeCampaignAudience(
	recipientsRaw string,
	skippedRaw string,
) ([]domain.CampaignRecipient, []domain.SkippedRow, error) {
	if strings.TrimSpace(recipientsRaw) == "" {
		return nil, nil, errors.New("recipients_data is required")
	}

	var recipients []domain.CampaignRecipient
	if err := decodeStrictCampaignJSON(recipientsRaw, &recipients); err != nil {
		return nil, nil, fmt.Errorf("invalid recipients_data: %w", err)
	}
	if len(recipients) == 0 {
		return nil, nil, errors.New("at least one recipient is required")
	}
	if len(recipients) > maxCampaignCSVRows {
		return nil, nil, fmt.Errorf("recipient count exceeds %d", maxCampaignCSVRows)
	}

	seenPhones := make(map[string]struct{}, len(recipients))
	for i := range recipients {
		canonicalPhone, valid := domain.SanitizePhone(recipients[i].To)
		if !valid {
			return nil, nil, fmt.Errorf("recipient %d has an invalid phone", i+1)
		}
		if _, duplicate := seenPhones[canonicalPhone]; duplicate {
			return nil, nil, fmt.Errorf("recipient %d duplicates another phone", i+1)
		}
		seenPhones[canonicalPhone] = struct{}{}
		recipients[i].To = canonicalPhone
		if recipients[i].Variables == nil {
			recipients[i].Variables = make(map[string]string)
		}
	}
	if err := validateCampaignRecipientVariables(recipients); err != nil {
		return nil, nil, err
	}

	var skipped []domain.SkippedRow
	if strings.TrimSpace(skippedRaw) != "" {
		if err := decodeStrictCampaignJSON(skippedRaw, &skipped); err != nil {
			return nil, nil, fmt.Errorf("invalid skipped_data: %w", err)
		}
	}
	if len(skipped) > maxCampaignCSVRows || len(recipients)+len(skipped) > maxCampaignCSVRows {
		return nil, nil, fmt.Errorf("audience row count exceeds %d", maxCampaignCSVRows)
	}
	for i, row := range skipped {
		if row.LineNumber < 1 ||
			len(row.RawInput) > maxCampaignSkippedRawBytes ||
			len(row.Reason) > maxCampaignSkippedReasonBytes {
			return nil, nil, fmt.Errorf("skipped row %d exceeds canonical limits", i+1)
		}
	}
	return recipients, skipped, nil
}

func decodeStrictCampaignJSON(raw string, destination any) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func validateCampaignRecipientVariables(recipients []domain.CampaignRecipient) error {
	for recipientIndex, recipient := range recipients {
		if len(recipient.Variables) > maxCampaignVariables {
			return fmt.Errorf(
				"recipient %d has more than %d variables",
				recipientIndex+1,
				maxCampaignVariables,
			)
		}
		totalBytes := 0
		for key, value := range recipient.Variables {
			if key == "" ||
				len(key) > maxCampaignVariableKeyBytes ||
				len(value) > maxCampaignVariableValueBytes {
				return fmt.Errorf("recipient %d contains an invalid variable", recipientIndex+1)
			}
			totalBytes += len(key) + len(value)
		}
		if totalBytes > maxCampaignVariablesBytes {
			return fmt.Errorf(
				"recipient %d variable data exceeds %d bytes",
				recipientIndex+1,
				maxCampaignVariablesBytes,
			)
		}
	}
	return nil
}

func campaignConnectionReady(conn *repository.Connection) bool {
	if conn == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(conn.Status)) {
	case "active", "connected":
		return true
	default:
		return false
	}
}

func (h *CampaignHandler) resolveCampaignWABATemplate(
	ctx context.Context,
	workspaceID uuid.UUID,
	connectionID uuid.UUID,
	selection string,
	language string,
) (*repository.WABATemplate, *campaignWABATemplateSnapshot, error) {
	if h.TemplateRepo == nil {
		return nil, nil, errors.New("WABA template repository is unavailable")
	}
	selection = strings.TrimSpace(selection)
	language = strings.TrimSpace(language)
	if selection == "" {
		return nil, nil, errors.New("an approved WABA template is required")
	}

	var (
		template *repository.WABATemplate
		err      error
	)
	if templateID, parseErr := uuid.Parse(selection); parseErr == nil {
		template, err = h.TemplateRepo.GetByID(ctx, templateID)
	} else if language != "" {
		template, err = h.TemplateRepo.GetByNameAndLanguage(
			ctx,
			connectionID,
			selection,
			language,
		)
	} else {
		var templates []repository.WABATemplate
		templates, err = h.TemplateRepo.ListByConnection(ctx, connectionID)
		if err == nil {
			for i := range templates {
				if templates[i].Name != selection {
					continue
				}
				if template != nil {
					return nil, nil, errors.New("WABA template language is ambiguous")
				}
				template = &templates[i]
			}
			if template == nil {
				err = repository.ErrTemplateNotFound
			}
		}
	}
	if err != nil || template == nil {
		return nil, nil, errors.New("WABA template not found")
	}
	if template.WorkspaceID != workspaceID || template.ConnectionID != connectionID {
		return nil, nil, errors.New("WABA template does not belong to the selected connection")
	}
	if !strings.EqualFold(strings.TrimSpace(template.Status), "APPROVED") {
		return nil, nil, errors.New("WABA template is not approved")
	}
	if !wabaTemplateLanguagePattern.MatchString(template.Language) {
		return nil, nil, errors.New("WABA template language is invalid")
	}

	parameterCount, err := validateCampaignWABATemplateComponents(template.Components)
	if err != nil {
		return nil, nil, err
	}
	return template, &campaignWABATemplateSnapshot{
		Language:           template.Language,
		BodyParameterCount: parameterCount,
	}, nil
}

func validateCampaignWABATemplateComponents(raw json.RawMessage) (int, error) {
	var components []json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &components) != nil || len(components) == 0 {
		return 0, errors.New("WABA template components are invalid")
	}

	bodyCount := 0
	parameterIndexes := make(map[int]struct{})
	for _, rawComponent := range components {
		var component campaignWABATemplateComponent
		if err := json.Unmarshal(rawComponent, &component); err != nil {
			return 0, errors.New("WABA template components are invalid")
		}
		componentType := strings.ToUpper(strings.TrimSpace(component.Type))
		if componentType == "" {
			return 0, errors.New("WABA template component type is missing")
		}
		if componentType != "BODY" {
			if unresolvedTemplateValuePattern.Match(rawComponent) {
				return 0, errors.New("dynamic non-body WABA components are not supported for campaigns")
			}
			continue
		}

		bodyCount++
		if bodyCount > 1 || strings.TrimSpace(component.Text) == "" {
			return 0, errors.New("WABA template must contain exactly one valid body component")
		}
		for _, placeholder := range unresolvedTemplateValuePattern.FindAllString(component.Text, -1) {
			match := wabaTemplatePlaceholderPattern.FindStringSubmatch(placeholder)
			if len(match) != 2 || match[0] != placeholder {
				return 0, errors.New("WABA template body contains a non-numeric parameter")
			}
			index, err := strconv.Atoi(match[1])
			if err != nil || index < 1 || index > maxCampaignVariables {
				return 0, errors.New("WABA template body parameter index is invalid")
			}
			parameterIndexes[index] = struct{}{}
		}
	}
	if bodyCount != 1 {
		return 0, errors.New("WABA template must contain exactly one valid body component")
	}

	for index := 1; index <= len(parameterIndexes); index++ {
		if _, ok := parameterIndexes[index]; !ok {
			return 0, errors.New("WABA template body parameters must be contiguous")
		}
	}
	return len(parameterIndexes), nil
}

func applyCampaignWABATemplateParameters(
	c *echo.Context,
	recipients []domain.CampaignRecipient,
	parameterCount int,
) error {
	for index := 1; index <= parameterCount; index++ {
		formKey := fmt.Sprintf("waba_param_%d", index)
		mapping := strings.TrimSpace(c.FormValue(formKey))
		if mapping == "" {
			return fmt.Errorf("WABA template parameter %d is required", index)
		}
		for recipientIndex := range recipients {
			resolved := domain.ResolveVariables(mapping, recipients[recipientIndex].Variables)
			if strings.TrimSpace(resolved) == "" || unresolvedTemplateValuePattern.MatchString(resolved) {
				return fmt.Errorf(
					"WABA template parameter %d cannot be resolved for recipient %d",
					index,
					recipientIndex+1,
				)
			}
			recipients[recipientIndex].Variables[strconv.Itoa(index)] = resolved
		}
	}
	for index := parameterCount + 1; index <= maxCampaignVariables; index++ {
		if strings.TrimSpace(c.FormValue(fmt.Sprintf("waba_param_%d", index))) != "" {
			return fmt.Errorf("WABA template parameter %d is not declared by the template", index)
		}
	}
	return validateCampaignRecipientVariables(recipients)
}

func validateCampaignWABATemplateRecipients(
	recipients []domain.CampaignRecipient,
	parameterCount int,
) error {
	for recipientIndex, recipient := range recipients {
		for parameterIndex := 1; parameterIndex <= parameterCount; parameterIndex++ {
			value, ok := recipient.Variables[strconv.Itoa(parameterIndex)]
			if !ok ||
				strings.TrimSpace(value) == "" ||
				unresolvedTemplateValuePattern.MatchString(value) {
				return fmt.Errorf(
					"WABA template parameter %d is invalid for recipient %d",
					parameterIndex,
					recipientIndex+1,
				)
			}
		}
	}
	return nil
}

func (h *CampaignHandler) Create(c *echo.Context) error {
	workspaceIDStr, err := echo.PathParam[string](c, "workspace_id")
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}
	workspaceID, err := uuid.Parse(workspaceIDStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}

	c.Request().Body = http.MaxBytesReader(
		c.Response(),
		c.Request().Body,
		maxCampaignCreateBodyBytes,
	)
	if err := c.Request().ParseForm(); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return c.String(http.StatusRequestEntityTooLarge, "campaign payload too large")
		}
		return c.String(http.StatusBadRequest, "invalid campaign payload")
	}

	name := c.FormValue("name")
	connectionIDStr := c.FormValue("channel")
	batchSizeStr := c.FormValue("batch_size")
	delayStr := c.FormValue("delay_seconds")

	batchSize, _ := strconv.Atoi(batchSizeStr)
	if batchSize <= 0 {
		batchSize = 100
	}
	if batchSize > maxCampaignBatchSize {
		return c.String(http.StatusBadRequest, "batch_size exceeds maximum of 1000")
	}
	delaySeconds := 5
	if delayStr != "" {
		delaySeconds, err = strconv.Atoi(delayStr)
		if err != nil || delaySeconds < 0 || delaySeconds > domain.CampaignMaxDelaySeconds {
			return c.String(
				http.StatusBadRequest,
				fmt.Sprintf("delay_seconds must be between 0 and %d", domain.CampaignMaxDelaySeconds),
			)
		}
	}

	recipients, skipped, err := decodeCampaignAudience(
		c.FormValue("recipients_data"),
		c.FormValue("skipped_data"),
	)
	if err != nil {
		return c.String(http.StatusBadRequest, fmt.Sprintf("invalid campaign audience: %v", err))
	}

	var connectionID uuid.UUID
	var channel string
	var conn *repository.Connection

	if parsedID, err := uuid.Parse(connectionIDStr); err == nil {
		connectionID = parsedID
		conn, err = h.ConnectionRepo.GetByIDForWorkspace(
			c.Request().Context(),
			workspaceID,
			connectionID,
		)
		if err != nil || conn == nil || conn.WorkspaceID != workspaceID {
			return c.String(http.StatusBadRequest, "connection not found")
		}
		channel = conn.Channel
	} else {
		// Fallback: treat connectionIDStr as channel name and get default connection
		conn, err = h.ConnectionRepo.GetDefaultChannelConnection(c.Request().Context(), workspaceID, connectionIDStr)
		if err != nil {
			return c.String(http.StatusBadRequest, fmt.Sprintf("no active connection found for channel %s: %v", connectionIDStr, err))
		}
		connectionID = conn.ID
		channel = conn.Channel
	}
	if !campaignConnectionReady(conn) {
		return c.String(http.StatusBadRequest, "connection must be active or connected")
	}

	var templateName *string
	var templateLanguage *string
	if channel == "whatsapp_cloud" {
		template, snapshot, templateErr := h.resolveCampaignWABATemplate(
			c.Request().Context(),
			workspaceID,
			connectionID,
			c.FormValue("template_select"),
			c.FormValue("template_language"),
		)
		if templateErr != nil {
			return c.String(http.StatusBadRequest, templateErr.Error())
		}
		tName := template.Name
		tLanguage := snapshot.Language
		templateName = &tName
		templateLanguage = &tLanguage
		if err := applyCampaignWABATemplateParameters(
			c,
			recipients,
			snapshot.BodyParameterCount,
		); err != nil {
			return c.String(http.StatusBadRequest, err.Error())
		}
	} else {
		body := c.FormValue("body_template")
		if body != "" {
			templateName = &body
		}
	}

	camp := &domain.Campaign{
		WorkspaceID:      workspaceID,
		ConnectionID:     &connectionID,
		Name:             name,
		Status:           domain.CampaignStatusDraft,
		BatchSize:        batchSize,
		DelaySeconds:     delaySeconds,
		TemplateName:     templateName,
		TemplateLanguage: templateLanguage,
		Channel:          &channel,
		Recipients:       recipients,
		SkippedRows:      skipped,
	}

	_, err = h.CampaignRepo.Create(c.Request().Context(), camp)
	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("failed to save campaign: %v", err))
	}

	// Redirect back to campaigns list page
	c.Response().Header().Set("HX-Redirect", fmt.Sprintf("/admin/workspaces/%s/campaigns", workspaceIDStr))
	return c.String(http.StatusOK, "")
}

func (h *CampaignHandler) DownloadSkipped(c *echo.Context) error {
	workspaceID, err := campaignPathWorkspaceID(c)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}
	idStr, err := echo.PathParam[string](c, "id")
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}

	camp, err := h.CampaignRepo.GetByID(c.Request().Context(), id)
	if err != nil {
		return c.String(http.StatusNotFound, "campaign not found")
	}
	if camp.WorkspaceID != workspaceID {
		return c.String(http.StatusNotFound, "campaign not found")
	}

	c.Response().Header().Set("Content-Type", "text/csv")
	c.Response().Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=campanha_%s_rejeitados.csv", id.String()[:8]))
	c.Response().WriteHeader(http.StatusOK)

	writer := csv.NewWriter(c.Response())
	_ = writer.Write([]string{"Linha", "Registro Original", "Motivo da Rejeicao"})

	for _, row := range camp.SkippedRows {
		_ = writer.Write([]string{
			strconv.Itoa(row.LineNumber),
			neutralizeCampaignCSVCell(row.RawInput),
			neutralizeCampaignCSVCell(row.Reason),
		})
	}
	writer.Flush()
	return nil
}

// neutralizeCampaignCSVCell prevents attacker-controlled exports from being
// interpreted as formulas by spreadsheet applications. The apostrophe is part
// of the CSV value and is intentionally added before any leading whitespace.
func neutralizeCampaignCSVCell(value string) string {
	trimmed := strings.TrimLeft(value, " \t\r\n")
	if trimmed == "" {
		return value
	}
	switch trimmed[0] {
	case '=', '+', '-', '@':
		return "'" + value
	default:
		return value
	}
}

func (h *CampaignHandler) Start(c *echo.Context) error {
	workspaceID, err := campaignPathWorkspaceID(c)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}
	idStr, err := echo.PathParam[string](c, "id")
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}

	ctx := c.Request().Context()
	camp, err := h.CampaignRepo.GetByID(ctx, id)
	if err != nil {
		return c.String(http.StatusNotFound, "campaign not found")
	}
	if camp.WorkspaceID != workspaceID {
		return c.String(http.StatusNotFound, "campaign not found")
	}

	if camp.Status == domain.CampaignStatusSending ||
		camp.Status == domain.CampaignStatusCompleted {
		return mw.Render(c, http.StatusOK, pages.CampaignRow(camp.WorkspaceID, *camp))
	}
	if camp.Status != domain.CampaignStatusDraft {
		return c.String(http.StatusBadRequest, "only campaigns in draft status can be started")
	}
	if camp.ConnectionID == nil {
		return c.String(http.StatusBadRequest, "campaign connection is missing")
	}
	connection, err := h.ConnectionRepo.GetByIDForWorkspace(
		ctx,
		camp.WorkspaceID,
		*camp.ConnectionID,
	)
	if err != nil || connection == nil {
		return c.String(http.StatusBadRequest, "campaign connection not found")
	}
	if !campaignConnectionReady(connection) {
		return c.String(http.StatusConflict, "campaign connection must be active or connected")
	}
	if camp.Channel == nil || *camp.Channel != connection.Channel {
		return c.String(http.StatusConflict, "campaign channel no longer matches its connection")
	}
	if len(camp.Recipients) == 0 {
		return c.String(http.StatusBadRequest, "campaign has no recipients")
	}

	var templateSnapshot *campaignWABATemplateSnapshot
	if connection.Channel == "whatsapp_cloud" {
		if camp.TemplateName == nil || camp.TemplateLanguage == nil {
			return c.String(http.StatusConflict, "campaign WABA template snapshot is incomplete")
		}
		_, templateSnapshot, err = h.resolveCampaignWABATemplate(
			ctx,
			camp.WorkspaceID,
			connection.ID,
			*camp.TemplateName,
			*camp.TemplateLanguage,
		)
		if err != nil {
			return c.String(http.StatusConflict, err.Error())
		}
		if err := validateCampaignWABATemplateRecipients(
			camp.Recipients,
			templateSnapshot.BodyParameterCount,
		); err != nil {
			return c.String(http.StatusConflict, err.Error())
		}
	}

	batches, err := buildCampaignBatches(camp, templateSnapshot, json.Marshal)
	if err != nil {
		return c.String(http.StatusInternalServerError, "failed to prepare campaign batches")
	}
	for _, batch := range batches {
		var task queue.CampaignBatchTask
		if err := json.Unmarshal(batch.Payload, &task); err != nil {
			return c.String(http.StatusInternalServerError, "failed to validate campaign batches")
		}
		if err := queue.ValidateCampaignBatchOutboundPayloads(
			camp,
			task,
			connection.ID,
			connection.SenderIdentity,
		); err != nil {
			if errors.Is(err, messagebus.ErrPayloadTooLarge) {
				return c.String(
					http.StatusRequestEntityTooLarge,
					"campaign message exceeds the supported delivery size",
				)
			}
			return c.String(http.StatusInternalServerError, "failed to validate campaign delivery")
		}
	}

	status, err := h.CampaignRepo.PrepareCampaignStart(
		ctx,
		camp.ID,
		camp.WorkspaceID,
		camp.UpdatedAt,
		batches,
	)
	if err != nil {
		if errors.Is(err, repository.ErrCampaignInvalidState) {
			return c.String(http.StatusBadRequest, "campaign cannot be started from its current state")
		}
		if errors.Is(err, repository.ErrCampaignBatchConflict) {
			return c.String(http.StatusConflict, "campaign batches conflict with the durable start snapshot")
		}
		return c.String(http.StatusInternalServerError, "failed to persist campaign batches")
	}
	camp.Status = status

	return mw.Render(c, http.StatusOK, pages.CampaignRow(camp.WorkspaceID, *camp))
}

func (h *CampaignHandler) Cancel(c *echo.Context) error {
	workspaceID, err := campaignPathWorkspaceID(c)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}
	idStr, err := echo.PathParam[string](c, "id")
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}

	ctx := c.Request().Context()
	err = h.CampaignRepo.CancelForWorkspace(ctx, id, workspaceID)
	if err != nil {
		if errors.Is(err, repository.ErrCampaignNotFound) {
			return c.String(http.StatusNotFound, "campaign not found")
		}
		if errors.Is(err, repository.ErrCampaignInvalidState) {
			return c.String(http.StatusConflict, "only active or scheduled campaigns can be cancelled")
		}
		return c.String(http.StatusInternalServerError, "failed to cancel campaign")
	}

	camp, err := h.CampaignRepo.GetByID(ctx, id)
	if err != nil {
		return c.String(http.StatusInternalServerError, "failed to load cancelled campaign")
	}

	return mw.Render(c, http.StatusOK, pages.CampaignRow(camp.WorkspaceID, *camp))
}

func (h *CampaignHandler) Delete(c *echo.Context) error {
	workspaceID, err := campaignPathWorkspaceID(c)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid workspace ID")
	}
	idStr, err := echo.PathParam[string](c, "id")
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return c.String(http.StatusBadRequest, "invalid campaign ID")
	}

	err = h.CampaignRepo.Delete(c.Request().Context(), id, workspaceID)
	if err != nil {
		if errors.Is(err, repository.ErrCampaignNotFound) {
			return c.String(http.StatusNotFound, "campaign not found")
		}
		if errors.Is(err, repository.ErrCampaignInvalidState) {
			return c.String(http.StatusConflict, "active campaigns must be cancelled before deletion")
		}
		return c.String(http.StatusInternalServerError, "failed to delete campaign")
	}

	return c.String(http.StatusOK, "")
}

func campaignPathWorkspaceID(c *echo.Context) (uuid.UUID, error) {
	workspaceIDStr, err := echo.PathParam[string](c, "workspace_id")
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(workspaceIDStr)
}

func buildCampaignBatches(
	campaign *domain.Campaign,
	templateSnapshot *campaignWABATemplateSnapshot,
	marshal func(any) ([]byte, error),
) ([]repository.CampaignBatch, error) {
	batchSize := campaign.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	// Split first by both recipient count and serialized bytes. Probe with the
	// maximum possible index width so replacing it with the real totals cannot
	// make the final payload larger. Binary search keeps this O(batches*log N)
	// instead of repeatedly serializing every growing prefix.
	chunks := make([][]domain.CampaignRecipient, 0)
	for start := 0; start < len(campaign.Recipients); {
		high := start + batchSize
		if high > len(campaign.Recipients) {
			high = len(campaign.Recipients)
		}
		low := start + 1
		fittingEnd := start
		for low <= high {
			middle := low + (high-low)/2
			probe := campaignBatchTask(
				campaign,
				templateSnapshot,
				maxCampaignCSVRows,
				maxCampaignCSVRows,
				campaign.Recipients[start:middle],
			)
			payload, err := marshal(probe)
			if err != nil {
				return nil, err
			}
			if len(payload) <= repository.MaxCampaignBatchPayloadBytes {
				fittingEnd = middle
				low = middle + 1
			} else {
				high = middle - 1
			}
		}
		if fittingEnd == start {
			return nil, errCampaignBatchTooLarge
		}
		chunks = append(chunks, campaign.Recipients[start:fittingEnd])
		start = fittingEnd
	}

	totalBatches := len(chunks)
	batches := make([]repository.CampaignBatch, 0, totalBatches)
	for index, recipients := range chunks {
		batchIndex := index + 1
		task := campaignBatchTask(
			campaign,
			templateSnapshot,
			batchIndex,
			totalBatches,
			recipients,
		)
		payload, err := marshal(task)
		if err != nil {
			return nil, err
		}
		if len(payload) > repository.MaxCampaignBatchPayloadBytes {
			return nil, errCampaignBatchTooLarge
		}
		batches = append(batches, repository.CampaignBatch{
			BatchIndex:   batchIndex,
			TotalBatches: totalBatches,
			TraceID:      fmt.Sprintf("campaign_%s_batch_%d", campaign.ID, batchIndex),
			Payload:      payload,
			DelaySeconds: campaign.DelaySeconds,
		})
	}
	return batches, nil
}

func campaignBatchTask(
	campaign *domain.Campaign,
	templateSnapshot *campaignWABATemplateSnapshot,
	batchIndex int,
	totalBatches int,
	recipients []domain.CampaignRecipient,
) queue.CampaignBatchTask {
	task := queue.CampaignBatchTask{
		CampaignID:   campaign.ID,
		WorkspaceID:  campaign.WorkspaceID,
		BatchIndex:   batchIndex,
		TotalBatches: totalBatches,
		Recipients:   recipients,
		DelaySeconds: campaign.DelaySeconds,
	}
	if templateSnapshot != nil {
		task.TemplateSnapshot = &queue.CampaignTemplateSnapshot{
			Language:           templateSnapshot.Language,
			BodyParameterCount: templateSnapshot.BodyParameterCount,
		}
	}
	return task
}
