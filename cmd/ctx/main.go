package ctx

type Context struct {
	Name           string `json:"name"`
	AdminURL       string `json:"admin_url"`        // например: http://core-pulsar01d.stage.core.amosrv.ru/admin/v2
	Token          string `json:"token"`            // Bearer-токен (опционально)
	Tenant         string `json:"tenant"`           // amocrm
	Namespace      string `json:"namespace"`        // core-dev
	Prefix         string `json:"prefix"`           // например "ahuzhamberdiev|"
	HTTPTimeoutSec int    `json:"http_timeout_sec"` // таймаут HTTP-запросов
}
