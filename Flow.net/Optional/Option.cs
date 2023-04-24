using System;

namespace Flow.Optional;

public struct Option<T> : IEquatable<Option<T>> where T : class
{
    private T? _content;

    public static Option<T> Some(T obj) => new() { _content = obj };
    public static Option<T> None() => new();

    public Option<TResult> Map<TResult>(Func<T, TResult> map) where TResult : class =>
        new() { _content = _content is not null ? map(_content) : null };
    public ValueOption<TResult> MapValue<TResult>(Func<T, TResult> map) where TResult : struct =>
        _content is not null ? ValueOption<TResult>.Some(map(_content)) : ValueOption<TResult>.None();

    public Option<TResult> MapOptional<TResult>(Func<T, Option<TResult>> map) where TResult : class =>
        _content is not null ? map(_content) : Option<TResult>.None();
    public ValueOption<TResult> MapOptionalValue<TResult>(Func<T, ValueOption<TResult>> map) where TResult : struct =>
        _content is not null ? map(_content) : ValueOption<TResult>.None();

    public T Reduce(T orElse) => _content ?? orElse;
    public T Reduce(Func<T> orElse) => _content ?? orElse();

    public Option<T> Where(Func<T, bool> predicate) =>
        _content is not null && predicate(_content) ? this : Option<T>.None();

    public Option<T> WhereNot(Func<T, bool> predicate) =>
        _content is not null && !predicate(_content) ? this : Option<T>.None();

    public override int GetHashCode() => _content?.GetHashCode() ?? 0;
    public override bool Equals(object? other) => other is Option<T> option && Equals(option);

    public bool Equals(Option<T> other) =>
        _content is null ? other._content is null
        : _content.Equals(other._content);

    public static bool operator ==(Option<T>? a, Option<T>? b) => a is null ? b is null : a.Equals(b);
    public static bool operator !=(Option<T>? a, Option<T>? b) => !(a == b);
}