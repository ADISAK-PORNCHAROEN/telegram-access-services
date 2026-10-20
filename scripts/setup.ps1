[CmdletBinding()]
param(
  [switch]$Docker,
  [switch]$DatabaseOnly,
  [string]$EnvironmentDirectory
)

$ErrorActionPreference = 'Stop'
if ($EnvironmentDirectory) {
  $root = [System.IO.Path]::GetFullPath($EnvironmentDirectory)
  New-Item -ItemType Directory -Force -Path $root | Out-Null
} else {
  $root = Split-Path -Parent $PSScriptRoot
}
$envFile = Join-Path $root '.env'
$written = [System.Collections.Generic.List[string]]::new()

function Get-EnvValue([string]$key) {
  if (-not (Test-Path $envFile)) { return '' }
  $line = Select-String -Path $envFile -Pattern ("^" + [regex]::Escape($key) + '=') | Select-Object -Last 1
  if (-not $line) { return '' }
  return ($line.Line -replace ("^" + [regex]::Escape($key) + '='), '')
}

function Set-EnvValue([string]$key, [string]$value) {
  $lines = [System.Collections.Generic.List[string]]::new()
  if (Test-Path $envFile) { $lines.AddRange([System.IO.File]::ReadAllLines($envFile)) }
  for ($i = $lines.Count - 1; $i -ge 0; $i--) {
    if ($lines[$i] -match ("^" + [regex]::Escape($key) + '=')) { $lines.RemoveAt($i) }
  }
  $lines.Add("$key=$value")
  $tempFile = "$envFile.tmp"
  [System.IO.File]::WriteAllLines($tempFile, $lines)
  Move-Item -Force $tempFile $envFile
  $written.Add($key) | Out-Null
  Write-Host "  Saved $key to .env" -ForegroundColor Green
}

function Read-Value([string]$key, [string]$prompt, [string]$pattern, [string]$hint, [switch]$Secret) {
  $current = Get-EnvValue $key
  while ($true) {
    if ($current) {
      Write-Host "  $prompt [press Enter to keep the current value] " -NoNewline
    } else {
      Write-Host "  $prompt " -NoNewline
    }
    if ($Secret) {
      $secure = Read-Host -AsSecureString
      $pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
      try { $value = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer) }
      finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }
      Write-Host ''
    } else {
      $value = Read-Host
    }
    if (-not $value -and $current) { $value = $current }
    if (-not $value) { Write-Host '  A value is required.' -ForegroundColor Yellow; continue }
    if ($value -notmatch $pattern) { Write-Host "  Invalid format. $hint" -ForegroundColor Yellow; continue }
    return $value
  }
}

function Ensure-DatabasePassword {
  $databaseCredential = Get-EnvValue 'POSTGRES_PASSWORD'
  if ($databaseCredential) {
    if ($databaseCredential -notmatch '^[A-Za-z0-9_-]{43}$') {
      throw 'The stored Docker database credential has an invalid format. Preserve the generated value or remove it to generate a new one.'
    }
    Write-Host '  Keeping the existing database credential.' -ForegroundColor DarkGray
    return
  }
  $bytes = New-Object byte[] 32
  $random = [System.Security.Cryptography.RandomNumberGenerator]::Create()
  try {
    $random.GetBytes($bytes)
    $databaseCredential = [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
  } finally {
    $random.Dispose()
    [Array]::Clear($bytes, 0, $bytes.Length)
  }
  Set-EnvValue 'POSTGRES_PASSWORD' $databaseCredential
}

if ($DatabaseOnly) {
  if (-not $Docker) { throw '-DatabaseOnly requires -Docker.' }
  Ensure-DatabasePassword
  Write-Host 'Database configuration is ready.' -ForegroundColor Green
  return
}

Write-Host "`nTelegram Access Services setup ($root)" -ForegroundColor Cyan
Write-Host 'The wizard stores credentials in .env. Do not share or commit that file.'

Write-Host "`n1. Telegram bot token" -ForegroundColor Cyan
Start-Process 'https://t.me/BotFather'
Write-Host 'Create a bot with /newbot, then copy its token.'
$botToken = Read-Value 'BOT_TOKEN' 'BOT_TOKEN' '^\d+:[A-Za-z0-9_-]{20,}$' 'Use the token issued by @BotFather.' -Secret
Set-EnvValue 'BOT_TOKEN' $botToken

Write-Host "`n2. Telegram API credentials" -ForegroundColor Cyan
Start-Process 'https://my.telegram.org'
Write-Host 'Open API development tools and copy the app ID and 32-character hash.'
$apiId = Read-Value 'API_ID' 'API_ID' '^\d{5,}$' 'Use the numeric app ID.'
Set-EnvValue 'API_ID' $apiId
$apiHash = Read-Value 'API_HASH' 'API_HASH' '^[0-9a-fA-F]{32}$' 'Use the 32-character API hash.' -Secret
Set-EnvValue 'API_HASH' $apiHash

Write-Host "`n3. Owner account" -ForegroundColor Cyan
Start-Process 'https://t.me/userinfobot'
Write-Host 'Get your numeric Telegram ID from @userinfobot. Choose a stable session name.'
$allowedId = Read-Value 'ALLOWED_USER_ID' 'ALLOWED_USER_ID' '^\d+$' 'Use your numeric Telegram user ID.'
Set-EnvValue 'ALLOWED_USER_ID' $allowedId
$sessionName = Read-Value 'SESSION_NAME' 'SESSION_NAME' '^[A-Za-z0-9_-]+$' 'Use English letters, numbers, underscore, or dash.'
Set-EnvValue 'SESSION_NAME' $sessionName

if ($Docker) { Ensure-DatabasePassword }
Write-Host "`nSetup complete. Updated: $($written -join ', ')" -ForegroundColor Green
Write-Host 'The .env file is private. Keep it with this installation and back it up securely.'
if ($Docker) {
  Write-Host 'Next: docker compose up -d --build'
  Write-Host 'Follow the app logs and open the one-time setup URL from a private network or through the printed SSH tunnel.'
  Write-Host 'Complete owner authorization in your browser once; the saved Telegram session persists in the app volume.'
  Write-Host 'Then: docker compose logs -f app'
} else {
  Write-Host 'Next: uv sync'
  Write-Host 'Then: uv run bot.py'
}
