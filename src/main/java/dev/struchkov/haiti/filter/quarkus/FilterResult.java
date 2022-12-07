package dev.struchkov.haiti.filter.quarkus;

import dev.struchkov.haiti.utils.Inspector;

import java.util.List;

public class FilterResult<T> {

    private final Long totalElements;
    private final Integer elements;
    private Long totalPages;
    private Integer page;
    private final List<T> content;

    private FilterResult(Builder<T> builder) {
        this.totalElements = builder.totalElements;
        this.elements = builder.elements;
        this.totalPages = builder.totalPages;
        this.page = builder.page;
        this.content = builder.content;
    }

    public Long getTotalElements() {
        return totalElements;
    }

    public Integer getElements() {
        return elements;
    }

    public Long getTotalPages() {
        return totalPages;
    }

    public Integer getPage() {
        return page;
    }

    public List<T> getContent() {
        return content;
    }

    public static <T> Builder<T> builder(
            Long totalElements,
            Integer elements,
            List<T> content
    ) {
        Inspector.isNotNull(totalElements, elements, content);
        return new FilterResult.Builder<>(totalElements, elements, content);
    }

    public static class Builder<T> {
        private final Long totalElements;
        private final Integer elements;
        private final List<T> content;
        private Long totalPages;
        private Integer page;

        private Builder(Long totalElements, Integer elements, List<T> content) {
            this.totalElements = totalElements;
            this.elements = elements;
            this.content = content;
        }

        public Builder<T> totalPages(Long totalPages) {
            this.totalPages = totalPages;
            return this;
        }

        public Builder<T> page(Integer page) {
            this.page = page;
            return this;
        }

        public FilterResult<T> build() {
            return new FilterResult<>(this);
        }

    }

    public <D> FilterResult<D> replaceContent(List<D> list) {
        return FilterResult.<D>builder(totalElements, elements, list)
                .page(page)
                .totalPages(totalPages)
                .build();
    }

}
