# MIT License

Copyright (c) 2024 RapidStreamer

**Permission is hereby granted**, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

**THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND**, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

---

**RapidStreamer NuGet Package**

By using this NuGet package, you agree to the terms of the MIT License as described above.

---
**Migrations**
---
***RapidStreamerWeb***

**Add**: 
- PostgreSql: 
  - dotnet ef migrations add InitialMigration --project .\Web\Infrastructure\Domains\RapidStreamer.Web.Infrastructure.Domains.PostgreSql\ --startup-project .\Web\RapidStreamer.Web\ --context RapidStreamerPostgreSqlDbContext -- "Server=192.168.1.141;Port=5432;Database=RapidStreamerWeb;User Id=postgres;Password=postgres1@3;"
- MySql: 
  - dotnet ef migrations add InitialMigration --project .\Web\Infrastructure\Domains\RapidStreamer.Web.Infrastructure.Domains.MySql\ --startup-project .\Web\RapidStreamer.Web\ --context RapidStreamerMySqlDbContext -- "Server=192.168.1.141;Port=3306;Database=RapidStreamerWeb;Uid=root;Pwd=password;"

**Update**: 
- dotnet ef database  update --project .\Web\Infrastructure\Domains\RapidStreamer.Web.Infrastructure.PostgreSql\ --startup-project .\Web\RapidStreamer.Web\ --context RapidStreamerDbContext -- "Server=192.168.1.141;Port=5432;Database=RapidStreamerWeb;User Id=postgres;Password=postgres1@3;"          