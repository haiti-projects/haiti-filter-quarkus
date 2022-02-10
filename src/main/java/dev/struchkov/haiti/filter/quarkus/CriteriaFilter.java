package dev.struchkov.haiti.filter.quarkus;

import dev.struchkov.haiti.filter.jooq.CriteriaJooqFilter;
import dev.struchkov.haiti.filter.jooq.CriteriaJooqQuery;
import dev.struchkov.haiti.filter.jooq.SortContainer;
import dev.struchkov.haiti.filter.jooq.SortType;
import dev.struchkov.haiti.filter.jooq.page.PageableOffset;
import dev.struchkov.haiti.filter.jooq.page.PageableSeek;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.conf.ParamType;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CriteriaFilter<T> {

    private final PgPool pgPool;
    private final CriteriaJooqFilter jooqFilter;
    private final Function<Row, T> mapper;
    private PageableOffset offset;

    private CriteriaFilter(PgPool pgPool, String table, DSLContext dslContext, Function<Row, T> mapper) {
        dslContext.settings().withParamType(ParamType.NAMED_OR_INLINED);
        this.pgPool = pgPool;
        this.mapper = mapper;
        this.jooqFilter = CriteriaJooqFilter.create(table, dslContext);
    }

    public static <T> CriteriaFilter<T> create(PgPool pgPool, String table, DSLContext dslContext, Function<Row, T> mapper) {
        return new CriteriaFilter<>(pgPool, table, dslContext, mapper);
    }

    public CriteriaFilter<T> and(CriteriaJooqQuery filterQuery) {
        jooqFilter.and(filterQuery);
        return this;
    }

    public CriteriaFilter<T> and(Consumer<CriteriaJooqQuery> query) {
        jooqFilter.and(query);
        return this;
    }


    public CriteriaFilter<T> or(CriteriaJooqQuery filterQuery) {
        jooqFilter.or(filterQuery);
        return this;
    }

    public CriteriaFilter<T> or(Consumer<CriteriaJooqQuery> query) {
        jooqFilter.or(query);
        return this;
    }

    public CriteriaFilter<T> page(PageableOffset offset) {
        jooqFilter.page(offset);
        this.offset = offset;
        return this;
    }

    public CriteriaFilter<T> page(PageableSeek seek) {
        jooqFilter.page(seek);
        return this;
    }

    public CriteriaFilter<T> sort(SortContainer container) {
        jooqFilter.sort(container);
        return this;
    }

    public CriteriaFilter<T> sort(String field, SortType sortType) {
        jooqFilter.sort(field, sortType);
        return this;
    }

    public CriteriaFilter<T> sort(String field) {
        jooqFilter.sort(field);
        return this;
    }

    public Uni<List<T>> build() {
        final Query query = jooqFilter.build();
        return pgPool.preparedQuery(query.getSQL())
                .execute()
                .map(rows ->
                        StreamSupport.stream(rows.spliterator(), false)
                                .map(mapper)
                                .collect(Collectors.toList())
                );
    }

    public Uni<Long> count() {
        final Query query = jooqFilter.count();
        return pgPool.preparedQuery(query.getSQL())
                .execute()
                .map(t -> t.iterator().next().getLong("count"));
    }

    public Uni<FilterResult<T>> filterResult() {
        final Uni<Long> count = count();
        final Uni<List<T>> content = build();
        return Uni.combine().all()
                .unis(count, content)
                .asTuple()
                .map(
                        t -> {
                            final Long totalElements = t.getItem1();
                            final List<T> results = t.getItem2();
                            return FilterResult.builder(
                                            totalElements, results.size(), results
                                    )
                                    .page(offset.getPageNumber())
                                    .totalPages(totalElements / results.size())
                                    .build();
                        }
                );
    }

}
