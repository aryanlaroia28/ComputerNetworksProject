# PowerShell script to demonstrate LiteDB Semantic Caching

# Helper function to send RESP commands to the server via TCP
function Send-RespCommand {
    param (
        [string]$Command,
        [string]$Argument
    )

    try {
        # Construct RESP String
        $respString = ""
        if ([string]::IsNullOrEmpty($Argument)) {
            # Single command (like SQLSTATS)
            $respString = "*1`r`n`${Command.Length}`r`n${Command}`r`n"
        } else {
            # Two-part command (SQL <query>)
            $respString = "*2`r`n`${Command.Length}`r`n${Command}`r`n`${Argument.Length}`r`n${Argument}`r`n"
        }

        # Create TCP client and connect
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $tcpClient.Connect("localhost", 6379)
        $stream = $tcpClient.GetStream()

        # Write the command
        $writer = New-Object System.IO.StreamWriter($stream, [System.Text.Encoding]::UTF8)
        $writer.Write($respString)
        $writer.Flush()

        # Give the server a moment to process
        Start-Sleep -Milliseconds 150

        # Read the response
        $buffer = New-Object byte[] 4096
        $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
        $response = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $bytesRead)

        # Clean up
        $writer.Close()
        $stream.Close()
        $tcpClient.Close()

        # Format and print the response
        # The response is a RESP bulk string: $length\r\nstring\r\n
        if ($response -match '\$(.+)\r\n([\s\S]+)\r\n') {
            # Use -replace to trim the final newline for cleaner printing
            $formattedOutput = $matches[2] -replace '\r?\n$', ''
            Write-Host $formattedOutput
        } elseif ($response -eq "$-1`r`n") {
            Write-Host "(Empty result)"
        } else {
            Write-Host $response # Fallback for PONG, OK, or ERR
        }

    } catch {
        Write-Host "ERROR: Connection failed. Is the server running?" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
    }
}

# --- THE TEST SCENARIO ---
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "      Demonstrating Semantic Caching: NOC Server Monitoring" -ForegroundColor Cyan
Write-Host "=================================================================="
Write-Host "SCENARIO: A Network Operations Center (NOC) analyst is monitoring"
Write-Host "server health. The database 'server_logs' is constantly updated."
Write-Host "Our cache (max size 5) will help speed up repeat queries."
Write-Host ""
Write-Host "We will first check the cache stats, which should be zero."
Write-Host "=================================================================="
Write-Host ""
Start-Sleep -Seconds 1

# 1. Initial State
Write-Host "------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host "EXECUTING COMMAND: SQLSTATS" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------------"
Send-RespCommand "SQLSTATS" ""
Write-Host ""
Start-Sleep -Seconds 1


# 2. The Cache Miss
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "STEP 1: The analyst runs a broad query to find all servers"
Write-Host "with high CPU load (> 80). This is a CACHE MISS."
Write-Host "We expect to see a ~100ms penalty."
Write-Host "=================================================================="
Write-Host ""
Start-Sleep -Seconds 1

$query1 = "SELECT * FROM server_logs WHERE cpu_load > 80"
Write-Host "------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host "EXECUTING COMMAND: SQL ""$query1""" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------------"
Send-RespCommand "SQL" $query1
Write-Host ""
Start-Sleep -Seconds 1

# 3. The Semantic Hit!
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "STEP 2: An alert fires! The analyst must *immediately* check"
Write-Host "for CRITICAL load (> 95). This is a SEMANTIC CACHE HIT."
Write-Host "The query is a subset of the (cpu_load > 80) query already in"
Write-Host "the cache. We expect this to be (almost) instantaneous."
Write-Host "=================================================================="
Write-Host ""
Start-Sleep -Seconds 1

$query2 = "SELECT server_name, cpu_load, status FROM server_logs WHERE cpu_load > 95"
Write-Host "------------------------------------------------------------------" -ForegroundColor Green
Write-Host "EXECUTING COMMAND: SQL ""$query2""" -ForegroundColor Green
Write-Host "------------------------------------------------------------------"
Send-RespCommand "SQL" $query2
Write-Host ""
Start-Sleep -Seconds 1

# 4. The Direct Hit
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "STEP 3: The analyst runs the original (cpu_load > 80) query"
Write-Host "again to refresh their dashboard. This is now a DIRECT CACHE HIT."
Write-Host "We expect this to also be (almost) instantaneous."
Write-Host "=================================================================="
Write-Host ""
Start-Sleep -Seconds 1

Write-Host "------------------------------------------------------------------" -ForegroundColor Green
Write-Host "EXECUTING COMMAND: SQL ""$query1""" -ForegroundColor Green
Write-Host "------------------------------------------------------------------"
Send-RespCommand "SQL" $query1
Write-Host ""
Start-Sleep -Seconds 1

# 5. Another Miss (to show cache is working)
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "STEP 4: A different analyst queries the 'users' table."
Write-Host "This is a new query and will be a CACHE MISS."
Write-Host "=================================================================="
Write-Host ""
Start-Sleep -Seconds 1

$query3 = "SELECT name, age FROM users WHERE age > 90"
Write-Host "------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host "EXECUTING COMMAND: SQL ""$query3""" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------------"
Send-RespCommand "SQL" $query3
Write-Host ""
Start-Sleep -Seconds 1

# 6. Final Stats
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "STEP 5: Let's check the final cache statistics."
Write-Host "We expect: 4 Queries, 1 Direct Hit, 1 Semantic Hit, 2 Misses."
Write-Host "=================================================================="
Write-Host ""
Start-Sleep -Seconds 1

Write-Host "------------------------------------------------------------------" -ForegroundColor Yellow
Write-Host "EXECUTING COMMAND: SQLSTATS" -ForegroundColor Yellow
Write-Host "------------------------------------------------------------------"
Send-RespCommand "SQLSTATS" ""
Write-Host ""
Start-Sleep -Seconds 1

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "DEMO COMPLETE." -ForegroundColor Cyan
Write-Host "=================================================================="