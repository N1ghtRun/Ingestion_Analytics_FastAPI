# Test rate limiting - make 110 requests quickly
for ($i = 1; $i -le 110; $i++) {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8000/stats/dau?from=2024-01-15&to=2024-01-16" -UseBasicParsing
        Write-Host "Request $i : Status $($response.StatusCode) | Remaining: $($response.Headers['X-RateLimit-Remaining'])"
    }
    catch {
        Write-Host "Request $i : RATE LIMITED (429)" -ForegroundColor Red
        break
    }
    Start-Sleep -Milliseconds 100
}
