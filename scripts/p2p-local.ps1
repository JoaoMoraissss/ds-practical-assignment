# Exercise 2 on a single machine: 4 peers run as local processes on different ports.
# Ctrl-C stops everything.

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\.."

# Add JDK to PATH
$env:PATH = "C:\Program Files\Java\jdk-27\bin;$env:PATH"

# Build if needed
if (-not (Test-Path "build\ds\assignment\p2p\Peer.class")) {
    Write-Host "Building project..."
    javac -d build src/ds/assignment/p2p/*.java
}

$BASE_PORT = 7000

$jobs = @()

Write-Host "Starting P2P Anti-Entropy with 4 peers..."
Write-Host "Press Ctrl+C to stop all processes"
Write-Host ""

# Start 4 peers
for ($i = 0; $i -lt 4; $i++) {
    $port = $BASE_PORT + $i
    $neighbours = @()

    # Each peer connects to all others
    for ($j = 0; $j -lt 4; $j++) {
        if ($j -ne $i) {
            $neighbours += "localhost:$($BASE_PORT + $j)"
        }
    }

    $args = @("-cp", "build", "ds.assignment.p2p.Peer") + $neighbours + @("--port", "$port", "--name", "p$i", "--rate", "4")

    $peer = Start-Process java -ArgumentList $args -NoNewWindow -PassThru
    $jobs += $peer
    Start-Sleep -Milliseconds 500
}

Write-Host "All processes started. Press Ctrl+C to stop."

# Wait for user to press Ctrl+C
try {
    Wait-Process -Id $jobs[0].Id
} finally {
    Write-Host "`nStopping all processes..."
    $jobs | ForEach-Object { Stop-Process $_.Id -Force -ErrorAction SilentlyContinue }
}
