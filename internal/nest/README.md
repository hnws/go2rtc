# Google Nest

[`new in v1.6.0`](https://github.com/AlexxIT/go2rtc/releases/tag/v1.6.0)

For simplicity, it is recommended to connect the Nest/WebRTC camera to the [Home Assistant](../hass/README.md). 
But if you can somehow get the below parameters, Nest/WebRTC source will work without Home Assistant.

```yaml
streams:
  nest-doorbell: nest:?client_id=***&client_secret=***&refresh_token=***&project_id=***&device_id=***
```

If a stream keeps dropping or fails to connect, enable debug logging for this module to see OAuth token refreshes, session extensions and Google's error responses:

```yaml
log:
  nest: debug
```
