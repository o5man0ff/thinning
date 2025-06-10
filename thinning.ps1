# Сценарий прореживания бэкапов с конечными объектами в виде файлов или бэкапов
#
# Giga.chat и mao 2025
#
# Структура JSON файла (образец):
#
#   [
#     {
#     "jobName": "teamcenterReportDB",
#     "type": "file",
#     "source": "I:\\FileBackups_WritedToTape\\MSSQL\\TCDB\\full\\ReportDB",
#     "retentionPolicy": {
#         "days": 14,
#         "weeks": 4,
#         "months": 12,
#         "years": 5
#       }
#     },
#     {
#     "jobName": "HanaScm",
#     "type": "folder",
#     "source": "I:\\FileBackups_WritedToTape\\HANA\\hana-scm\\full",
#     "retentionPolicy": {
#         "days": 7,
#         "weeks": 4,
#         "months": 6,
#         "years": 1
#       }
#     },
#     {
#     "jobName": "BSE",
#     "type": "file",
#     "source": "I:\\FileBackups_WritedToTape\\MSSQL\\BSE\\full\\process_architect_azimut",
#     "retentionPolicy": {
#         "weeks": 4,
#         "months": 6,
#         "years": 5
#       }
#     }
#   ]
#
#-------------------------------------------------------------------

# Подключение конфигурации
$configPath = 'C:\scripts\Thinning\retentionPolicy.json'

if (!(Test-Path $configPath)) {
    Write-Error "Файл конфигурации retentionPolicy.json не найден!"
    exit
}

try {
    $jobsConfig = Get-Content -Raw -Path $configPath | ConvertFrom-Json
} catch {
    Write-Error "Ошибка чтения конфигурационного файла: $_"
    exit
}

foreach ($job in $jobsConfig) {
    # Загружаем данные из конфига
    $jobName = $job.jobName
    $sourceDir = $job.source
    $objectType = $job.type
    $policyDays = $job.retentionPolicy.days
    $policyWeeks = $job.retentionPolicy.weeks
    $policyMonths = $job.retentionPolicy.months
    $policyYears = $job.retentionPolicy.years

    Write-Host "Начало обработки задания '$jobName' в директории '$sourceDir'..."

    # Зависимость от типа объекта (file/folder)
    switch ($objectType) {
        "file" {
            # ОБРАБОТКА ФАЙЛОВ
            $items = @(Get-ChildItem -Path $sourceDir | Sort-Object LastWriteTime -Descending)
        }
        "folder" {
            # ОБРАБОТКА ПАПОК
            $items = @(Get-ChildItem -Directory -Path $sourceDir | Sort-Object LastWriteTime -Descending)
        }
        default {
            Write-Warning "Тип объекта '$objectType' неизвестен. Пропускаем задание."
            continue
        }
    }

    if (-not $items.Count) {
        Write-Warning "В директории '$sourceDir' нет элементов типа '$objectType'"
        continue
    }

    # По итогу собран список файлов/папок от свежих к старым

    # Начальные списки
    $keptItems = @()
    $itemsForDeletion = @()

    # ЕСЛИ СУЩЕСТВУЕТ ПОЛИТИКА "DAILY": отсчитываются первые N групп (группировка по дням) и содержимое этих групп добавляется в список сохраняемых
    # Определяем группы уникальных дней и ограничиваем выборку по числу дней
    if ($null -ne $policyDays -and $policyDays -gt 0) {
        Write-Verbose "Применяем правило DAYS..." -Verbose

        # Формируем группы по уникальным дням (LastWriteTime), сортируем по убыванию свежести
        $groupedByDay = $items | Group-Object { $_.LastWriteTime.Date } | Sort-Object Name -Descending

        # Выбираем только первые N групп (дней)
        $selectedGroups = $groupedByDay | Select-Object -First $policyDays

        # Собираем все объекты из выбранных групп
        foreach ($group in $selectedGroups) {
            $keptItems += $group.Group
        }
    }


    # ЕСЛИ СУЩЕСТВУЕТ ПОЛИТИКА "WEEKS": 
    if ($null -ne $policyWeeks -and $policyWeeks -gt 0) {
        Write-Verbose "Применяем правило WEEKS..." -Verbose
        $groupedByWeek = $items | Group-Object { [System.Globalization.CultureInfo]::InvariantCulture.Calendar.GetWeekOfYear($_.LastWriteTime, [System.Globalization.CalendarWeekRule]::FirstFourDayWeek, [DayOfWeek]::Monday)}
        # Нейросеть обрабатывала все группы ($groupedByWeek), добавил перебор только нужного количества ($groupedByWeek[0..$limitWeeks])
        $limitWeeks = $policyWeeks-1
        foreach ($group in $groupedByWeek[0..$limitWeeks]) {
            # Берём самый свежий элемент из каждой недели
            $keptItems += $group.Group[-1]
        }
    }

    # ЕСЛИ СУЩЕСТВУЕТ ПОЛИТИКА "MONTHS":
    if ($null -ne $policyMonths -and $policyMonths -gt 0) {
        Write-Verbose "Применяем правило MONTHS..." -Verbose
        $groupedByMonth = $items | Group-Object { $_.LastWriteTime.Year }, { $_.LastWriteTime.Month }
        # Нейросеть обрабатывала все группы ($groupedByMonth), добавил перебор только нужного количества ($groupedByMonth[0..$limitMonths])
        $limitMonths = $policyMonths - 1
        foreach ($group in $groupedByMonth[0..$limitMonths]) {
            # Берём самую раннюю копию месяца (здесь тоже стоял 0, и брался самый последний бэкап в месяце, поставил -1, чтобы брать самый первый с начала месяца)
            # тут еще нюанс, что берется период не в 12 месяцев, а в 12 последних месячных точек, т.е. если в N месяцев нет бэкапов, то на эти N месяцев будет
            # увеличена глубина прохода по месяцам
            $keptItems += $group.Group[-1]
        }
    }

    # ЕСЛИ СУЩЕСТВУЕТ ПОЛИТИКА "YEARS":
    if ($null -ne $policyYears -and $policyYears -gt 0) {
        Write-Verbose "Применяем правило YEARS..." -Verbose
        $groupedByYear = $items | Group-Object { $_.LastWriteTime.Year }
        # Нейросеть обрабатывала все группы ($groupedByYear), добавил перебор только нужного количества ($groupedByYear[0..$limitYears])
        $limitYears = $policyYears-1
        foreach ($group in $groupedByYear[0..$limitYears]) {
            # Берём самую раннюю копию года (здесь тоже стоял 0, и брался самый последний бэкап в году, поставил -1, чтобы брать самый первый с начала года)
            $keptItems += $group.Group[-1]
        }
    }

    # Исключаем сохранённые элементы из общего списка
    $itemsForDeletion = $items | Where-Object { $keptItems -notcontains $_ }

    # Регистрируем элементы, планируемые к удалению
    Write-Host "`nПланируется удалить элементы:"
    $itemsForDeletion | ForEach-Object { Write-Host "- $($_)" }

    # Удаляем элементы
    foreach ($item in $itemsForDeletion) {
        try {
            if ($objectType -eq "file") {
                Remove-Item -Path $item.FullName -Force
            }
            else {
                # Рекурсивное удаление папок
                Remove-Item -Recurse -Path $item.FullName -Force
            }
            Write-Output "$($item.Name) удалён"
        }
        catch {
            Write-Error "Ошибка удаления элемента $($item.Name): $_"
        }
    }

    Write-Host "Задание '$jobName' успешно обработано!"
}
