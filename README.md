# pubsub

[![Go Reference](https://pkg.go.dev/badge/github.com/augustjourney/pubsub.svg)](https://pkg.go.dev/github.com/augustjourney/pubsub)
[![CI](https://github.com/augustjourney/pubsub/actions/workflows/ci.yml/badge.svg)](https://github.com/augustjourney/pubsub/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/augustjourney/pubsub)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Маленькая потокобезопасная in-memory pub/sub шина на Go с обобщённым типом сообщения, drop-политикой для медленных получателей и корректным завершением. Без внешних зависимостей.

## Установка

```bash
go get github.com/augustjourney/pubsub
```

## Быстрый старт

```go
package main

import (
    "fmt"
    "time"

    "github.com/augustjourney/pubsub"
)

func main() {
    bus := pubsub.New[string](5)
    defer bus.Shutdown()

    ch := bus.Subscribe("news")
    go func() {
        for msg := range ch {
            fmt.Println("получено:", msg)
        }
    }()

    bus.Publish("news", "привет")
    bus.Publish("news", "мир")

    time.Sleep(50 * time.Millisecond)
}
```

Полный пример — в [examples/basic](examples/basic/main.go).

## API

| Функция | Описание |
|---|---|
| `New[T any](bufSize int) *PubSub[T]` | Создаёт новую шину. `bufSize` — размер буфера канала каждого подписчика; при `0` берётся значение по умолчанию. |
| `(*PubSub[T]).Subscribe(topic string) <-chan T` | Регистрирует подписчика. После `Shutdown` возвращает `nil`. |
| `(*PubSub[T]).Unsubscribe(topic string, ch <-chan T)` | Удаляет подписку и закрывает её канал. Безопасен для чужих/несуществующих каналов. |
| `(*PubSub[T]).Publish(topic string, msg T)` | Рассылает сообщение всем подписчикам. Не блокируется. |
| `(*PubSub[T]).Shutdown()` | Закрывает все каналы и помечает шину как остановленную. Идемпотентен. |

Подробная документация — на [pkg.go.dev](https://pkg.go.dev/github.com/augustjourney/pubsub).

## Особенности

- **Generics.** `PubSub[T any]` — любой тип сообщения, типобезопасно, без `interface{}`/`any`-кастов.
- **Slow consumer protection.** Если буфер подписчика заполнен, сообщение для него отбрасывается, чтобы не блокировать публикатор и остальных подписчиков. Размер буфера задаётся в `New`.
- **Корректный shutdown.** `Shutdown` потокобезопасен и идемпотентен (через `sync.Once`); работающие в этот момент `Publish` гарантированно завершаются до закрытия каналов (через `sync.RWMutex`).
- **Потокобезопасность.** Все методы можно вызывать из любого числа горутин одновременно; тесты прогоняются с `-race`.
- **Без зависимостей.** Только стандартная библиотека Go.

## Тесты

```bash
go test -race -count=1 ./...
```

## Лицензия

[MIT](LICENSE).
