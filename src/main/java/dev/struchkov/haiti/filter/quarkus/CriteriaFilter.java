package dev.struchkov.haiti.filter.quarkus;

import dev.struchkov.haiti.filter.Filter;
import dev.struchkov.haiti.filter.FilterQuery;
import dev.struchkov.haiti.filter.jooq.CriteriaJooqFilter;
import dev.struchkov.haiti.filter.jooq.JoinTable;
import dev.struchkov.haiti.filter.jooq.SortContainer;
import dev.struchkov.haiti.filter.jooq.SortType;
import dev.struchkov.haiti.filter.jooq.page.PageableOffset;
import dev.struchkov.haiti.filter.jooq.page.PageableSeek;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import lombok.NonNull;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.conf.ParamType;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

public class CriteriaFilter<T> implements Filter {

    private final PgPool pgPool;
    private final CriteriaJooqFilter jooqFilter;
    private final Function<Row, T> mapper;

    private CriteriaFilter(PgPool pgPool, String table, DSLContext dslContext, Function<Row, T> mapper) {
        dslContext.settings().withParamType(ParamType.NAMED_OR_INLINED);
        this.pgPool = pgPool;
        this.mapper = mapper;
        this.jooqFilter = CriteriaJooqFilter.create(table, dslContext);
    }

    public static <T> CriteriaFilter<T> create(PgPool pgPool, String table, DSLContext dslContext, Function<Row, T> mapper) {
        return new CriteriaFilter<>(pgPool, table, dslContext, mapper);
    }

    @Override
    public Filter and(FilterQuery filterQuery) {
        jooqFilter.and(filterQuery);
        return this;
    }

    @Override
    public Filter and(Consumer<FilterQuery> query) {
        jooqFilter.and(query);
        return this;
    }

    @Override
    public Filter or(FilterQuery filterQuery) {
        jooqFilter.or(filterQuery);
        return this;
    }

    @Override
    public Filter or(Consumer<FilterQuery> query) {
        jooqFilter.or(query);
        return this;
    }

    @Override
    public Filter not(FilterQuery filterQuery) {
        jooqFilter.not(filterQuery);
        return this;
    }

    @Override
    public Filter not(Consumer<FilterQuery> query) {
        jooqFilter.not(query);
        return this;
    }

    public CriteriaFilter<T> page(@NonNull PageableOffset offset) {
        jooqFilter.page(offset);
        return this;
    }

    public CriteriaFilter<T> page(@NonNull PageableSeek seek) {
        jooqFilter.page(seek);
        return this;
    }

    public CriteriaFilter<T> join(@NonNull JoinTable... joinTables) {
        jooqFilter.join(joinTables);
        return this;
    }

    public CriteriaFilter<T> sort(@NonNull SortContainer container) {
        jooqFilter.sort(container);
        return this;
    }

    public CriteriaFilter<T> sort(@NonNull String field, SortType sortType) {
        jooqFilter.sort(field, sortType);
        return this;
    }

    public CriteriaFilter<T> sort(@NonNull String field) {
        jooqFilter.sort(field);
        return this;
    }

    @Override
    public Multi<T> build() {
        final Query query = jooqFilter.build();
        return pgPool.preparedQuery(query.getSQL())
                .execute()
                .onItem()
                .transformToMulti(rows -> Multi.createFrom().items(
                        StreamSupport.stream(rows.spliterator(), false)
                ))
                .map(mapper);
    }
}
