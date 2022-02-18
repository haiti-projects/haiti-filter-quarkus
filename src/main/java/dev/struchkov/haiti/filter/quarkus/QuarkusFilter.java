package dev.struchkov.haiti.filter.quarkus;

import dev.struchkov.haiti.filter.jooq.CriteriaJooqFilter;
import dev.struchkov.haiti.filter.jooq.CriteriaJooqQuery;
import dev.struchkov.haiti.filter.jooq.join.JoinTable;
import dev.struchkov.haiti.filter.jooq.page.PageableOffset;
import dev.struchkov.haiti.filter.jooq.page.PageableSeek;
import dev.struchkov.haiti.filter.jooq.sort.SortContainer;
import dev.struchkov.haiti.filter.jooq.sort.SortType;
import dev.struchkov.haiti.utils.Assert;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.conf.ParamType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class QuarkusFilter<ID> {

    public static final String COLUMN_COUNT = "count";
    private final String tableName;
    private boolean distinctCount;

    private final PgPool pgPool;
    private final CriteriaJooqFilter jooqFilter;
    private PageableOffset offset;
    private final List<String> sortFieldNames = new ArrayList<>();

    private QuarkusFilter(PgPool pgPool, String table, DSLContext dslContext) {
        dslContext.settings().withParamType(ParamType.NAMED_OR_INLINED);
        this.tableName = table;
        this.pgPool = pgPool;
        this.jooqFilter = CriteriaJooqFilter.create(table, dslContext);
    }

    public static <ID> QuarkusFilter<ID> create(PgPool pgPool, String table, DSLContext dslContext) {
        return new QuarkusFilter<>(pgPool, table, dslContext);
    }

    public QuarkusFilter<ID> and(CriteriaJooqQuery filterQuery) {
        jooqFilter.and(filterQuery);
        return this;
    }

    public QuarkusFilter<ID> and(Consumer<CriteriaJooqQuery> query) {
        jooqFilter.and(query);
        return this;
    }

    public QuarkusFilter<ID> or(CriteriaJooqQuery filterQuery) {
        jooqFilter.or(filterQuery);
        return this;
    }

    public QuarkusFilter<ID> or(Consumer<CriteriaJooqQuery> query) {
        jooqFilter.or(query);
        return this;
    }

    public QuarkusFilter<ID> join(JoinTable... joinTables) {
        Assert.isNotNull(joinTables);
        jooqFilter.join(joinTables);
        return this;
    }

    public QuarkusFilter<ID> page(PageableOffset offset) {
        jooqFilter.page(offset);
        this.offset = offset;
        return this;
    }

    public QuarkusFilter<ID> page(PageableSeek seek) {
        jooqFilter.page(seek);
        return this;
    }

    public QuarkusFilter<ID> sort(SortContainer container) {
        if (container != null && container.getFieldName() != null) {
            jooqFilter.sort(container);
            sortFieldNames.add(container.getFieldName());
        }
        return this;
    }

    public QuarkusFilter<ID> sort(String field, SortType sortType) {
        if (field != null) {
            jooqFilter.sort(field, sortType);
            sortFieldNames.add(field);
        }
        return this;
    }

    public QuarkusFilter<ID> sort(String field) {
        if (field != null) {
            jooqFilter.sort(field);
            sortFieldNames.add(field);
        }
        return this;
    }

    public Uni<List<ID>> build(Class<ID> idType, String idField) {
        final Query query = jooqFilter.generateQuery(idField);
        final String sql = query.getSQL();
        return pgPool.preparedQuery(sql)
                .execute()
                .map(rows ->
                        StreamSupport.stream(rows.spliterator(), false)
                                .map(row -> row.get(idType, idField))
                                .collect(Collectors.toList())
                );
    }

    public Uni<Long> count() {
        final Query query = jooqFilter.generateCount();
        final String sql = query.getSQL();
        return pgPool.preparedQuery(sql)
                .execute()
                .map(t -> {
                    final long count = StreamSupport.stream(t.spliterator(), false).count();
                    if (distinctCount) {
                        return count;
                    } else {
                        final Optional<Long> optCount = StreamSupport.stream(t.spliterator(), false)
                                .map(row -> row.getLong(COLUMN_COUNT))
                                .reduce(((a, b) -> a * b));
                        return optCount.orElse(0L);
                    }
                });
    }

    public Uni<FilterResult<ID>> filterResult(Class<ID> idType, String idField) {
        jooqFilter.groupBy(idField);
        jooqFilter.groupBy(sortFieldNames);
        distinctCount = true;
        final Uni<Long> count = count();
        final Uni<List<ID>> content = build(idType, idField);
        return Uni.combine().all()
                .unis(count, content)
                .asTuple()
                .map(
                        t -> {
                            final Long totalElements = t.getItem1();
                            final List<ID> results = t.getItem2();
                            return FilterResult.builder(
                                            totalElements, results.size(), results
                                    )
                                    .page(offset.getPageNumber())
                                    .totalPages(results.isEmpty() ? offset.getPageNumber() : totalElements / results.size())
                                    .build();
                        }
                );
    }

}
