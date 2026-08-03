package nest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Logger is disabled by default so pkg/nest stays silent when used outside
// go2rtc. internal/nest wires it up to app.GetLogger("nest") at Init.
var Logger = zerolog.Nop()

// httpTimeout was previously written as `time.Second * 5000` (~83 minutes)
// everywhere in this file - almost certainly a typo for 5 seconds. A single
// hung request under the old value could stall the extend loop for over an
// hour, since ExtendStream/refreshToken run synchronously inside it.
const httpTimeout = 5 * time.Second

type API struct {
	Token     string
	ExpiresAt time.Time

	StreamProjectID string
	StreamDeviceID  string
	StreamExpiresAt time.Time

	// WebRTC
	StreamSessionID string

	// RTSP
	StreamToken          string
	StreamExtensionToken string

	key string // credentials cache key, used to refresh the OAuth token

	extendTimer *time.Timer
	extendStop  chan struct{}
	extendDone  chan struct{}
}

type Auth struct {
	AccessToken string
}

type DeviceInfo struct {
	Name      string
	DeviceID  string
	Protocols []string
}

var cache = map[string]*API{}
var cacheMu sync.Mutex

func NewAPI(clientID, clientSecret, refreshToken string) (*API, error) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	key := clientID + ":" + clientSecret + ":" + refreshToken
	now := time.Now()

	// The cache only stores the OAuth token. Each caller gets its own API
	// instance, because the Stream* fields hold per-stream session state -
	// multiple cameras sharing one instance overwrite each other's session
	// and only the last one gets extended.
	if api := cache[key]; api != nil && now.Before(api.ExpiresAt) {
		Logger.Debug().Time("expires_at", api.ExpiresAt).Msg("nest: reuse cached oauth token")
		return &API{Token: api.Token, ExpiresAt: api.ExpiresAt, key: key}, nil
	}

	Logger.Debug().Msg("nest: requesting oauth token")

	data := url.Values{
		"grant_type":    []string{"refresh_token"},
		"client_id":     []string{clientID},
		"client_secret": []string{clientSecret},
		"refresh_token": []string{refreshToken},
	}

	client := &http.Client{Timeout: httpTimeout}
	res, err := client.PostForm("https://www.googleapis.com/oauth2/v4/token", data)
	if err != nil {
		Logger.Warn().Err(err).Msg("nest: oauth token request failed")
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		Logger.Warn().Int("status", res.StatusCode).Str("body", string(b)).Msg("nest: oauth token request rejected")
		return nil, errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		AccessToken string        `json:"access_token"`
		ExpiresIn   time.Duration `json:"expires_in"`
		Scope       string        `json:"scope"`
		TokenType   string        `json:"token_type"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		Logger.Warn().Err(err).Msg("nest: oauth token response decode failed")
		return nil, err
	}

	api := &API{
		Token:     resv.AccessToken,
		ExpiresAt: now.Add(resv.ExpiresIn * time.Second),
	}

	Logger.Debug().Time("expires_at", api.ExpiresAt).Msg("nest: oauth token obtained")

	cache[key] = api

	return &API{Token: api.Token, ExpiresAt: api.ExpiresAt, key: key}, nil
}

func (a *API) GetDevices(projectID string) ([]DeviceInfo, error) {
	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" + projectID + "/devices"
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := &http.Client{Timeout: httpTimeout}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		Devices []Device
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return nil, err
	}

	devices := make([]DeviceInfo, 0, len(resv.Devices))

	for _, device := range resv.Devices {
		// only RTSP and WEB_RTC available (both supported)
		if len(device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols) == 0 {
			continue
		}

		i := strings.LastIndexByte(device.Name, '/')
		if i <= 0 {
			continue
		}

		name := device.Traits.SdmDevicesTraitsInfo.CustomName
		// Devices configured through the Nest app use the container/room name as opposed to the customName trait
		if name == "" && len(device.ParentRelations) > 0 {
			name = device.ParentRelations[0].DisplayName
		}

		devices = append(devices, DeviceInfo{
			Name:      name,
			DeviceID:  device.Name[i+1:],
			Protocols: device.Traits.SdmDevicesTraitsCameraLiveStream.SupportedProtocols,
		})
	}

	return devices, nil
}

// sdpMaxRetries/sdpRetryDelay tune the 409/429/401 backoff below - vars
// (not consts) so tests can shrink them instead of running at real-world
// timescales (30s, 60s, 120s in production).
var (
	sdpMaxRetries = 3
	sdpRetryDelay = 30 * time.Second
)

func (a *API) ExchangeSDP(projectID, deviceID, offer string) (string, error) {
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			Offer string `json:"offerSdp"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateWebRtcStream"
	reqv.Params.Offer = offer

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"

	maxRetries := sdpMaxRetries
	retryDelay := sdpRetryDelay

	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
		if err != nil {
			return "", err
		}

		req.Header.Set("Authorization", "Bearer "+a.Token)

		client := &http.Client{Timeout: httpTimeout}
		res, err := client.Do(req)
		if err != nil {
			Logger.Warn().Err(err).Str("device", deviceID).Int("attempt", attempt+1).Msg("nest: exchange sdp request failed")
			return "", err
		}

		// Handle 409 (Conflict), 429 (Too Many Requests), and 401 (Unauthorized)
		if res.StatusCode == 409 || res.StatusCode == 429 || res.StatusCode == 401 {
			res.Body.Close()
			Logger.Warn().Int("status", res.StatusCode).Str("device", deviceID).Int("attempt", attempt+1).
				Msg("nest: exchange sdp rejected, retrying")
			if attempt < maxRetries-1 {
				// Get new token from Google
				if err := a.refreshToken(); err != nil {
					Logger.Warn().Err(err).Msg("nest: token refresh before retry failed")
					return "", err
				}
				time.Sleep(retryDelay)
				retryDelay *= 2 // exponential backoff
				continue
			}
		}

		defer res.Body.Close()

		if res.StatusCode != 200 {
			b, _ := io.ReadAll(res.Body)
			Logger.Warn().Int("status", res.StatusCode).Str("device", deviceID).Str("body", string(b)).Msg("nest: exchange sdp failed")
			return "", errors.New("nest: wrong status: " + res.Status)
		}

		var resv struct {
			Results struct {
				Answer         string    `json:"answerSdp"`
				ExpiresAt      time.Time `json:"expiresAt"`
				MediaSessionID string    `json:"mediaSessionId"`
			} `json:"results"`
		}

		if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
			return "", err
		}

		a.StreamProjectID = projectID
		a.StreamDeviceID = deviceID
		a.StreamSessionID = resv.Results.MediaSessionID
		a.StreamExpiresAt = resv.Results.ExpiresAt

		Logger.Debug().Str("device", deviceID).Time("expires_at", a.StreamExpiresAt).
			Msg("nest: webrtc session established")

		return resv.Results.Answer, nil
	}

	return "", errors.New("nest: max retries exceeded")
}

func (a *API) refreshToken() error {
	if a.key == "" {
		return errors.New("nest: unable to find cached credentials")
	}

	Logger.Debug().Msg("nest: refreshing oauth token")

	// Parse credentials from the cache key
	parts := strings.SplitN(a.key, ":", 3)
	if len(parts) != 3 {
		return errors.New("nest: invalid cache key format")
	}
	clientID, clientSecret, refreshToken := parts[0], parts[1], parts[2]

	// Get new API instance which will refresh the token
	newAPI, err := NewAPI(clientID, clientSecret, refreshToken)
	if err != nil {
		return err
	}

	// Update current API with new token
	a.Token = newAPI.Token
	a.ExpiresAt = newAPI.ExpiresAt
	return nil
}

func (a *API) ExtendStream() error {
	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			MediaSessionID       string `json:"mediaSessionId,omitempty"`
			StreamExtensionToken string `json:"streamExtensionToken,omitempty"`
		} `json:"params"`
	}

	if a.StreamToken != "" {
		// RTSP
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendRtspStream"
		reqv.Params.StreamExtensionToken = a.StreamExtensionToken
	} else {
		// WebRTC
		reqv.Command = "sdm.devices.commands.CameraLiveStream.ExtendWebRtcStream"
		reqv.Params.MediaSessionID = a.StreamSessionID
	}

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		a.StreamProjectID + "/devices/" + a.StreamDeviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := &http.Client{Timeout: httpTimeout}
	res, err := client.Do(req)
	if err != nil {
		Logger.Warn().Err(err).Str("device", a.StreamDeviceID).Msg("nest: extend stream request failed")
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		Logger.Warn().Int("status", res.StatusCode).Str("device", a.StreamDeviceID).Str("body", string(b)).
			Msg("nest: extend stream rejected")
		return errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		Results struct {
			ExpiresAt            time.Time `json:"expiresAt"`
			MediaSessionID       string    `json:"mediaSessionId"`
			StreamExtensionToken string    `json:"streamExtensionToken"`
			StreamToken          string    `json:"streamToken"`
		} `json:"results"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return err
	}

	a.StreamSessionID = resv.Results.MediaSessionID
	a.StreamExpiresAt = resv.Results.ExpiresAt
	a.StreamExtensionToken = resv.Results.StreamExtensionToken
	a.StreamToken = resv.Results.StreamToken

	return nil
}

func (a *API) GenerateRtspStream(projectID, deviceID string) (string, error) {
	var reqv struct {
		Command string   `json:"command"`
		Params  struct{} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.GenerateRtspStream"

	b, err := json.Marshal(reqv)
	if err != nil {
		return "", err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		projectID + "/devices/" + deviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := &http.Client{Timeout: httpTimeout}
	res, err := client.Do(req)
	if err != nil {
		Logger.Warn().Err(err).Str("device", deviceID).Msg("nest: generate rtsp stream request failed")
		return "", err
	}

	if res.StatusCode != 200 {
		b, _ := io.ReadAll(res.Body)
		Logger.Warn().Int("status", res.StatusCode).Str("device", deviceID).Str("body", string(b)).
			Msg("nest: generate rtsp stream rejected")
		return "", errors.New("nest: wrong status: " + res.Status)
	}

	var resv struct {
		Results struct {
			StreamURLs           map[string]string `json:"streamUrls"`
			StreamExtensionToken string            `json:"streamExtensionToken"`
			StreamToken          string            `json:"streamToken"`
			ExpiresAt            time.Time         `json:"expiresAt"`
		} `json:"results"`
	}

	if err = json.NewDecoder(res.Body).Decode(&resv); err != nil {
		return "", err
	}

	if _, ok := resv.Results.StreamURLs["rtspUrl"]; !ok {
		return "", errors.New("nest: failed to generate rtsp url")
	}

	a.StreamProjectID = projectID
	a.StreamDeviceID = deviceID
	a.StreamToken = resv.Results.StreamToken
	a.StreamExtensionToken = resv.Results.StreamExtensionToken
	a.StreamExpiresAt = resv.Results.ExpiresAt

	Logger.Debug().Str("device", deviceID).Time("expires_at", a.StreamExpiresAt).
		Msg("nest: rtsp session established")

	return resv.Results.StreamURLs["rtspUrl"], nil
}

func (a *API) StopRTSPStream() error {
	if a.StreamProjectID == "" || a.StreamDeviceID == "" {
		return errors.New("nest: tried to stop rtsp stream without a project or device ID")
	}

	var reqv struct {
		Command string `json:"command"`
		Params  struct {
			StreamExtensionToken string `json:"streamExtensionToken"`
		} `json:"params"`
	}
	reqv.Command = "sdm.devices.commands.CameraLiveStream.StopRtspStream"
	reqv.Params.StreamExtensionToken = a.StreamExtensionToken

	b, err := json.Marshal(reqv)
	if err != nil {
		return err
	}

	uri := "https://smartdevicemanagement.googleapis.com/v1/enterprises/" +
		a.StreamProjectID + "/devices/" + a.StreamDeviceID + ":executeCommand"
	req, err := http.NewRequest("POST", uri, bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+a.Token)

	client := &http.Client{Timeout: httpTimeout}
	res, err := client.Do(req)
	if err != nil {
		Logger.Warn().Err(err).Str("device", a.StreamDeviceID).Msg("nest: stop rtsp stream request failed")
		return err
	}

	if res.StatusCode != 200 {
		Logger.Warn().Int("status", res.StatusCode).Str("device", a.StreamDeviceID).Msg("nest: stop rtsp stream rejected")
		return errors.New("nest: wrong status: " + res.Status)
	}

	a.StreamProjectID = ""
	a.StreamDeviceID = ""
	a.StreamExtensionToken = ""
	a.StreamToken = ""

	return nil
}

type Device struct {
	Name string `json:"name"`
	Type string `json:"type"`
	//Assignee string `json:"assignee"`
	Traits struct {
		SdmDevicesTraitsInfo struct {
			CustomName string `json:"customName"`
		} `json:"sdm.devices.traits.Info"`
		SdmDevicesTraitsCameraLiveStream struct {
			VideoCodecs        []string `json:"videoCodecs"`
			AudioCodecs        []string `json:"audioCodecs"`
			SupportedProtocols []string `json:"supportedProtocols"`
		} `json:"sdm.devices.traits.CameraLiveStream"`
		//SdmDevicesTraitsCameraImage struct {
		//	MaxImageResolution struct {
		//		Width  int `json:"width"`
		//		Height int `json:"height"`
		//	} `json:"maxImageResolution"`
		//} `json:"sdm.devices.traits.CameraImage"`
		//SdmDevicesTraitsCameraPerson struct {
		//} `json:"sdm.devices.traits.CameraPerson"`
		//SdmDevicesTraitsCameraMotion struct {
		//} `json:"sdm.devices.traits.CameraMotion"`
		//SdmDevicesTraitsDoorbellChime struct {
		//} `json:"sdm.devices.traits.DoorbellChime"`
		//SdmDevicesTraitsCameraClipPreview struct {
		//} `json:"sdm.devices.traits.CameraClipPreview"`
	} `json:"traits"`
	ParentRelations []struct {
		Parent      string `json:"parent"`
		DisplayName string `json:"displayName"`
	} `json:"parentRelations"`
}

// Tuning knobs for the extend loop below, kept as vars (not consts) so
// tests can shrink them instead of running at real-world timescales.
var (
	// extendMinDelay is a floor on the re-arm delay so a bogus or
	// already-passed StreamExpiresAt can't turn the timer into a hot loop.
	extendMinDelay = 30 * time.Second
	// extendRetryDelay is how soon a failed extend attempt is retried.
	// Transient errors (network blips, Google 5xx, OAuth timeouts) should
	// not be treated as fatal - only give up after several attempts.
	extendRetryDelay = 15 * time.Second
	extendMaxRetries = 10
)

func (a *API) StartExtendStreamTimer() {
	if a.extendTimer != nil {
		return
	}

	// Google expires sessions after ~5 minutes; each successful extension
	// returns a new expiresAt, so keep extending until the stream stops.
	timer := time.NewTimer(extendDelay(a.StreamExpiresAt))
	stop := make(chan struct{})
	done := make(chan struct{})
	a.extendTimer = timer
	a.extendStop = stop
	a.extendDone = done

	Logger.Debug().Str("device", a.StreamDeviceID).Time("expires_at", a.StreamExpiresAt).
		Msg("nest: extend timer started")

	go func() {
		defer close(done)

		failures := 0

		for {
			select {
			case <-timer.C:
				// The OAuth token lives ~1 hour, sessions can live longer
				if time.Now().After(a.ExpiresAt.Add(-30 * time.Second)) {
					if err := a.refreshToken(); err != nil {
						failures++
						Logger.Warn().Err(err).Str("device", a.StreamDeviceID).Int("attempt", failures).
							Msg("nest: refresh oauth token before extend failed, retrying")

						if failures >= extendMaxRetries {
							Logger.Error().Str("device", a.StreamDeviceID).
								Msg("nest: giving up on extending stream after repeated token refresh failures, stream will disconnect")
							return
						}

						timer.Reset(extendRetryDelay)
						continue
					}
				}

				if err := a.ExtendStream(); err != nil {
					failures++
					Logger.Warn().Err(err).Str("device", a.StreamDeviceID).Int("attempt", failures).
						Msg("nest: extend stream failed, retrying")

					if failures >= extendMaxRetries {
						Logger.Error().Str("device", a.StreamDeviceID).
							Msg("nest: giving up on extending stream after repeated failures, stream will disconnect")
						return
					}

					timer.Reset(extendRetryDelay)
					continue
				}

				failures = 0
				Logger.Debug().Str("device", a.StreamDeviceID).Time("expires_at", a.StreamExpiresAt).
					Msg("nest: extend stream ok")

				timer.Reset(extendDelay(a.StreamExpiresAt))
			case <-stop:
				return
			}
		}
	}()
}

// extendDelay returns how long to wait before the next extend attempt,
// never less than extendMinDelay - StreamExpiresAt can be zero/stale on the
// first arm or briefly inconsistent, and without a floor that turns the
// timer into a hot loop hammering the Google API.
func extendDelay(expiresAt time.Time) time.Duration {
	// Compare before subtracting time.Minute: time.Until on a zero/far-past
	// expiresAt clamps to the minimum representable Duration, and
	// subtracting from that would overflow back around to a huge positive
	// number instead of staying negative.
	if d := time.Until(expiresAt); d > extendMinDelay+time.Minute {
		return d - time.Minute
	}
	return extendMinDelay
}

// StopExtendStreamTimer stops the extend loop and waits for its goroutine to
// exit before returning, so callers can safely read/mutate the Stream*
// fields (e.g. to send a final StopRtspStream) right after this returns
// without racing the loop's last in-flight ExtendStream call.
func (a *API) StopExtendStreamTimer() {
	if a.extendTimer != nil {
		a.extendTimer.Stop()
		a.extendTimer = nil
	}
	if a.extendStop != nil {
		close(a.extendStop)
		a.extendStop = nil
	}
	if a.extendDone != nil {
		<-a.extendDone
		a.extendDone = nil
	}

	Logger.Debug().Str("device", a.StreamDeviceID).Msg("nest: extend timer stopped")
}
