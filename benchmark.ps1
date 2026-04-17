# 性能对比测试
$symbol = "BTCUSDT"
$interval = "1m"
$iterations = 100

Write-Host "开始性能测试..." -ForegroundColor Green
Write-Host "测试 $iterations 次请求到 /download/$symbol/$interval"

$times = @()
for ($i = 1; $i -le $iterations; $i++) {
    $start = Get-Date
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:30000/download/$symbol/$interval" -UseBasicParsing
    $end = Get-Date
    $duration = ($end - $start).TotalMilliseconds
    $times += $duration

    if ($i % 10 -eq 0) {
        Write-Host "完成 $i/$iterations"
    }
}

$avg = ($times | Measure-Object -Average).Average
$min = ($times | Measure-Object -Minimum).Minimum
$max = ($times | Measure-Object -Maximum).Maximum

Write-Host "`n性能统计:" -ForegroundColor Cyan
Write-Host "平均响应时间: $([math]::Round($avg, 2)) ms"
Write-Host "最快响应: $([math]::Round($min, 2)) ms"
Write-Host "最慢响应: $([math]::Round($max, 2)) ms"
