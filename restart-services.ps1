param(
  [int]$PythonPort = 3000,
  [int]$ReactPort = 5173,
  [int[]]$PortsToFree = @(),
  [string]$PythonCommand = "python src\server.py",
  [string]$ReactCommand = "",
  [switch]$NoStartReact
)

$ErrorActionPreference = "Stop"

function Stop-Ports {
  param([int[]]$Ports)
  foreach ($port in ($Ports | Where-Object { $_ -and $_ -gt 0 } | Select-Object -Unique)) {
    try {
      $pids = @(Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique)
      foreach ($pid in ($pids | Where-Object { $_ -and $_ -gt 0 } | Select-Object -Unique)) {
        try { Stop-Process -Id $pid -Force -ErrorAction SilentlyContinue } catch {}
      }
      Start-Sleep -Milliseconds 250
      Write-Host "Puerto $port liberado."
    } catch {
      Write-Host "No se pudo inspeccionar/liberar el puerto $port."
    }
  }
}

function Start-ServiceWindow {
  param(
    [string]$Title,
    [string]$WorkingDirectory,
    [string]$Command
  )
  $cmd = "Set-Location -LiteralPath '$WorkingDirectory'; $Command"
  Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoExit", "-Command", $cmd) -WindowStyle Normal | Out-Null
  Write-Host "Iniciado: $Title"
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "Deteniendo servicios y liberando puertos..."
Stop-Ports -Ports (@($PythonPort, $ReactPort) + @($PortsToFree))

Write-Host "Iniciando Python en puerto $PythonPort..."
$pyCmd = "`$env:PORT='$PythonPort'; $PythonCommand"
Start-ServiceWindow -Title "Python" -WorkingDirectory $root -Command $pyCmd

if (-not $NoStartReact) {
  if ([string]::IsNullOrWhiteSpace($ReactCommand)) {
    Write-Host "ReactCommand vacío; no se inicia React. Si necesitas React, ejecuta: .\\restart-services.ps1 -ReactCommand 'npm run dev'"
  } else {
    Write-Host "Iniciando React en puerto $ReactPort..."
    $reactCmd = "`$env:PORT='$ReactPort'; $ReactCommand"
    Start-ServiceWindow -Title "React" -WorkingDirectory $root -Command $reactCmd
  }
}

Write-Host "Listo."
