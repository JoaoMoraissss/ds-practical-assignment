# Exercise 1 on a single machine: the server and the 5 peers of the ring run as
# local processes on different ports. Ctrl-C stops everything.

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\.."

# Add JDK to PATH
$env:PATH = "C:\Program Files\Java\jdk-27\bin;$env:PATH"

# Build if needed
if (-not (Test-Path "build\ds\assignment\tring\Peer.class")) {
    Write-Host "Building project..."
    javac -d build src/ds/assignment/tring/*.java
}

$SERVER_PORT = 6000
$BASE_PORT = 5000   # peer pN listens on BASE_PORT + N

$jobs = @()

Write-Host "Starting Token Ring..."
Write-Host "Press Ctrl+C to stop all processes"
Write-Host ""

# Start server
$server = Start-Process java -ArgumentList "-cp","build","ds.assignment.tring.Server",$SERVER_PORT -NoNewWindow -PassThru
$jobs += $server
Start-Sleep -Seconds 1

# Start 5 peers
for ($i = 1; $i -le 5; $i++) {
    $next = ($i % 5) + 1
    $args = @("-cp", "build", "ds.assignment.tring.Peer",
              "localhost:$($BASE_PORT + $next)", "localhost:$SERVER_PORT",
              "--port", "$($BASE_PORT + $i)", "--name", "p$i")

    if ($i -eq 1) {
        $args += "--token"
    }

    $peer = Start-Process java -ArgumentList $args -NoNewWindow -PassThru
    $jobs += $peer
}

Write-Host "All processes started. Press Ctrl+C to stop."

# Wait for user to press Ctrl+C
try {
    Wait-Process -Id $jobs[0].Id
} finally {
    Write-Host "`nStopping all processes..."
    $jobs | ForEach-Object { Stop-Process $_.Id -Force -ErrorAction SilentlyContinue }
}
