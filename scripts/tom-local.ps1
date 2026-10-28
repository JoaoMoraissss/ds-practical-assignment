# Exercise 3 on a single machine: 6 peers run as local processes on different
# ports, each one knowing all the others. Ctrl-C stops everything.
#
#   .\scripts\tom-local.ps1           6 honest peers
#   .\scripts\tom-local.ps1 past      p6 reuses old timestamps
#   .\scripts\tom-local.ps1 future    p6 uses timestamps far ahead
param([string]$Evil = "")

$ErrorActionPreference = "Stop"
Set-Location "$PSScriptRoot\.."

New-Item -ItemType Directory -Force build | Out-Null
javac -d build (Get-ChildItem -Recurse src -Filter *.java | ForEach-Object FullName)

$BASE_PORT = 8000
$jobs = @()

for ($i = 1; $i -le 6; $i++) {
    $others = @()
    for ($j = 1; $j -le 6; $j++) {
        if ($j -ne $i) { $others += "localhost:$($BASE_PORT + $j)" }
    }
    $javaArgs = @("-cp", "build", "ds.assignment.tom.Peer") + $others + @("--port", "$($BASE_PORT + $i)", "--name", "p$i")
    if ($i -eq 6 -and $Evil -ne "") { $javaArgs += @("--evil", $Evil) }
    $jobs += Start-Process java -ArgumentList $javaArgs -NoNewWindow -PassThru
}

try {
    Wait-Process -Id $jobs[0].Id
} finally {
    $jobs | ForEach-Object { Stop-Process $_.Id -Force -ErrorAction SilentlyContinue }
}
