<#
.SYNOPSIS
  Publish NuGet packages and symbols to a feed.

.DESCRIPTION
  PowerShell script that handles:
   - Downloading artifacts from GitHub Actions
   - Publishing .nupkg files to NuGet feed
   - Publishing .snupkg symbol files
   - Deleting all workflow artifacts
   - Cleanup

  Used by GitHub Actions publish jobs.

.PARAMETER NuGetSource
  NuGet feed source URL

.PARAMETER NuGetApiKey
  NuGet feed API key

.PARAMETER PackagesPath
  Path where packages are downloaded (default: ./dist/packages)

.PARAMETER SymbolsPath
  Path where symbols are downloaded (default: ./dist/symbols)

.PARAMETER SkipSymbols
  Skip publishing symbol packages

.PARAMETER SkipCleanup
  Skip cleanup of downloaded artifacts

.EXAMPLE
  pwsh .github/scripts/publish-packages.ps1 -NuGetSource $env:NUGET_SOURCE -NuGetApiKey $env:NUGET_API_KEY
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$NuGetSource,
    
    [Parameter(Mandatory = $true)]
    [string]$NuGetApiKey,
    
    [string]$PackagesPath = './dist/packages',
    [string]$SymbolsPath = './dist/symbols',
    [switch]$SkipSymbols,
    [switch]$SkipCleanup
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host "=== NuGet Package Publishing ===" -ForegroundColor Cyan
Write-Host "Feed: $NuGetSource"
Write-Host "Packages Path: $PackagesPath"

# Validate required parameters
if (-not $NuGetSource -or -not $NuGetApiKey) {
    Write-Error "NuGetSource and NuGetApiKey are required."
    exit 1
}

# Push .nupkg files
Write-Host "`n--- Publishing Packages (.nupkg) ---" -ForegroundColor Yellow
$packages = Get-ChildItem -Path $PackagesPath -Filter '*.nupkg' -ErrorAction SilentlyContinue

if (-not $packages) {
    Write-Warning "No .nupkg files found in $PackagesPath"
}
else {
    Write-Host "Found $($packages.Count) package(s) to publish"
    $successCount = 0
    $skipCount = 0
    $failCount = 0
    
    foreach ($pkg in $packages) {
        Write-Host "`nPushing: $($pkg.Name)" -ForegroundColor Cyan
        
        dotnet nuget push $pkg.FullName `
            --source $NuGetSource `
            --api-key $NuGetApiKey `
            --skip-duplicate `
            2>&1 | Tee-Object -Variable output
        
        if ($LASTEXITCODE -eq 0) {
            if ($output -match 'already exists') {
                Write-Host "  ✓ Skipped (already exists)" -ForegroundColor Yellow
                $skipCount++
            }
            else {
                Write-Host "  ✓ Published successfully" -ForegroundColor Green
                $successCount++
            }
        }
        else {
            Write-Warning "  ✗ Failed to publish $($pkg.Name)"
            $failCount++
        }
    }
    
    Write-Host "`nPackage Summary:" -ForegroundColor Yellow
    Write-Host "  Published: $successCount" -ForegroundColor Green
    Write-Host "  Skipped: $skipCount" -ForegroundColor Yellow
    if ($failCount -gt 0) {
        Write-Host "  Failed: $failCount" -ForegroundColor Red
    }
}

# Push .snupkg files (symbols)
if (-not $SkipSymbols) {
    Write-Host "`n--- Publishing Symbols (.snupkg) ---" -ForegroundColor Yellow
    $symbols = Get-ChildItem -Path $SymbolsPath -Filter '*.snupkg' -ErrorAction SilentlyContinue
    
    if (-not $symbols) {
        Write-Host "No .snupkg files found in $SymbolsPath" -ForegroundColor Gray
    }
    else {
        Write-Host "Found $($symbols.Count) symbol package(s) to publish"
        
        foreach ($sym in $symbols) {
            Write-Host "`nPushing: $($sym.Name)" -ForegroundColor Cyan
            
            dotnet nuget push $sym.FullName `
                --source $NuGetSource `
                --api-key $NuGetApiKey `
                --skip-duplicate `
                2>&1 | Out-Null
            
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  ✓ Published successfully" -ForegroundColor Green
            }
            else {
                Write-Warning "  ✗ Failed to publish $($sym.Name) (non-fatal)"
            }
        }
    }
}

# Cleanup
if (-not $SkipCleanup) {
    Write-Host "`n--- Cleanup ---" -ForegroundColor Yellow
    
    if (Test-Path './dist') {
        Remove-Item -Path './dist' -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "Removed ./dist" -ForegroundColor Gray
    }
    
    if (Test-Path './artifacts') {
        Remove-Item -Path './artifacts' -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "Removed ./artifacts" -ForegroundColor Gray
    }
}

Write-Host "`n=== Publishing Complete ===" -ForegroundColor Green
exit 0
