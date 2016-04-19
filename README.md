# journald remote forwarder
This is a simple journald-forwarder for the systemd journald daemon. It uses
the CoreOS sdjournal C-bindings for Go (which unfortunately means it does have
a C-Go dependency for deployment, specific to your hosts).

This is a very minimal daemon: it implements checkpointing and JSON output only
(using the Go serialization as it reads journal files directly).

Example service file:
```
[Unit]
Description=journald remote JSON forwarding agent
After=local-fs.target network.target

[Service]
ExecStartPre=-/bin/mkdir /var/lib/journald-forwarder
ExecStart=/usr/local/bin/journald-forwarder -remote tcp://some.remote.host:5011
WorkingDirectory=/var/lib/journald-forwarder
Restart=always
RestartSec=1m

[Install]
WantedBy=multi-user.target
```
