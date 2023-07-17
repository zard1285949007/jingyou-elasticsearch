<?php

declare(strict_types=1);

namespace Liguizhou\Elasticsearch;

use App\Util\Base\Log;
use Hyperf\Utils\Collection;

/**
 * Class Builder
 * @package Elasticsearch
 * Date : 2023/7/14
 * Author: lgz
 * Desc: ElasticSearch 链式操作
 */
class Builder
{
    //查询条件符
    const OPERATE = ['=', '>', '<', '>=', '<=', '!=', '<>', 'in', 'not in', 'like', 'between', 'exists'];

    /**
     * 查询字段
     * @var array
     */
    protected array $fields = [];

    /**
     * 排序
     * @var array
     */
    protected array $order = [];

    /**
     * 查询条件
     * @var array
     */
    protected array $where = [];

    /**
     * 分组
     * @var array
     */
    protected array $group = [];

    /**
     * 页数
     * @var int
     */
    protected int $page = 1;

    /**
     * 条数
     * @var int
     */
    protected int $size = 0;

    /**
     * 是否聚合查询，聚合查询和普通查询的处理逻辑不一样
     * @var int
     */
    protected int $isAgg = 0;

    /**
     * 查询sql
     * @var array
     */
    protected array $sql = [];

    /**
     * @var Model
     */
    protected Model $model;

    protected $client;

    /**
     * 查询条件
     * @param array|string $column
     * @param null|mixed $operator
     * @param null|mixed $value
     * @return $this
     */
    public function where($column, $operator = null, $value = null): Builder
    {
        if (is_array($column)) {
            foreach ($column as $value) {
                list ($column, $operator, $value) = $value;
                $this->where($column, $operator, $value);
            }
        } else {
            if ($value == null) {
                $value = $operator;
                $operator = '=';
            }
            if (in_array($operator, self::OPERATE)) {
                $this->where = array_merge($this->where, [[$column, $operator, $value]]);
            }
        }

        return $this;
    }

    /**
     * 查询条件
     * @param array|string $column
     * @param null|mixed $operator
     * @param null|mixed $value
     * @return $this
     */
    public function andWhere($column, $operator = null, $value = null): Builder
    {
        return $this->where($column, $operator, $value);
    }

    public function whereBetween(string $column, array $value): Builder
    {
        return $this->where($column, 'between', $value);
    }

    public function whereIn(string $column, array $value): Builder
    {
        return $this->where($column, 'in', $value);
    }

    public function whereNotIn(string $column, array $value): Builder
    {
        return $this->where($column, 'not in', $value);
    }

    /**
     * 排序
     * @param string $column
     * @param $direction
     * @return $this
     */
    public function orderBy(string $column, $direction = 'asc'): Builder
    {
        $direction = in_array($direction, ['asc', 'desc']) ? $direction : 'asc';
        $this->order = array_merge($this->order, [['sort' => $column, 'order' => $direction]]);

        //排序需要将字段添加到查询字段中
        if (!isset($this->fields[$column])) {
            $fieldClass = new Field($column);
            $this->fields[$fieldClass->aliasField] = $fieldClass;
        }

        return $this;
    }

    /**
     * 分组
     * @param $column
     * @return $this
     */
    public function groupBy($column): Builder
    {
        $column = (array)$column;
        $this->group = array_merge($this->order, $column);
        $this->isAgg = 1;

        //分组需要将字段添加到查询字段中
        foreach ($column as $vColumn) {
            if (!isset($this->fields[$vColumn])) {
                $fieldClass = new Field($vColumn);
                $this->fields[$fieldClass->aliasField] = $fieldClass;
            }
        }

        return $this;
    }

    public function select(...$params): Builder
    {
        $fields = func_get_args();
        foreach ($fields as $field) {
            $fieldClass = new Field($field);
            $this->fields[$fieldClass->aliasField] = $fieldClass;
            if ($fieldClass->isAgg != 0) {
                $this->isAgg = 1;
            }
        }

        return $this;
    }

    private function sqlCombine()
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
        ];
        //获取查询条件
        $query = $this->parseQuery();
        if (!empty($query)) {
            $this->sql['body']['query'] = $query;
        }
        if ($this->isAgg != 0) {
            $this->aggSqlCombile();
        } else {
            $this->normalSqlCombine();
        }
    }

    /**
     * 非聚合查询拼接
     * @return void
     */
    private function normalSqlCombine()
    {
        //拼接查询字段
        $fieldArr = [];
        foreach ($this->fields as $vField) {
            if ($vField instanceof Field) {
                $fieldArr[] = $vField->field;
            }
        }
        if (!empty($fieldArr)) {
            $this->sql['body']['_source'] = $fieldArr;
        }

        //拼接排序
        $sort = [];
        foreach ($this->order as $vOrder) {
            $sort[] = [$vOrder['sort'] => $vOrder['order']];
        }

        if (!empty($sort)) {
            $this->sql['body']['sort'] = $sort;
        }

        //拼接分页
        if ($this->size != 0) { //等于0查所有数据
            $this->sql['body']['size'] = $this->size;
            $this->sql['body']['from'] = (($this->page - 1) * $this->size) ?: 0;
        }
    }

    /**
     * 聚合查询拼接
     * @return void
     */
    private function aggSqlCombile()
    {
        $aggQuery = [];
        //分组
        $group = $this->parseGroup();
        if (!empty($group)) {
            $aggQuery['terms'] = $group;
        }

        //拼接查询字段
        $fieldArr = [];
        foreach ($this->fields as $vField) {
            if ($vField instanceof Field) {
                if ($vField->isAgg) {
                    $fieldArr[$vField->aliasField] = [
                        $vField->aggType => [
                            'field' => $vField->field
                        ]
                    ];
                } else {
                    $fieldArr[$vField->aliasField] = [
                        'max' => [
                            'field' => $vField->field
                        ]
                    ];
                }

            }
        }
        if (!empty($fieldArr)) {
            $aggQuery['aggs'] = $fieldArr;
        }

        //拼接排序
        if (!empty($group)) { //没有分组不需要排序
            $sort = [];
            foreach ($this->order as $vOrder) {
                $sort[] = [$vOrder['sort'] => ['order' => $vOrder['order']]];
            }
            if (!empty($sort)) {
                $aggQuery['aggs']['self_sort']['bucket_sort']['sort'] = $sort;
            }
        }

        //拼接分页
        if (!empty($group)) { //没有分组不需要分页
            if ($this->size != 0) {
                $aggQuery['aggs']['self_sort']['bucket_sort']['size'] = $this->size;
                $aggQuery['aggs']['self_sort']['bucket_sort']['from'] = (($this->page - 1) * $this->size) ?: 0;
            }
        }

        if (empty($group)) {
            $this->sql['body'] = $aggQuery;
        } else {
            $this->sql['body']['aggs']['self_group'] = $aggQuery;
        }

        $this->sql['body']['size'] = 0; //聚合不需要详细数据

    }

    /**
     * 分组格式
     * @return array|array[]
     */
    private function parseGroup(): array
    {
        $group = [];
        $groupString = '';
        foreach ($this->group as $vGroup) {
            $groupString .= "doc['{$vGroup}'].value + '_'";
        }

        if (!empty($groupString)) {
            $group = [
                'script' => [
                    "lang"   => "painless",
                    'source' => $groupString
                ]
            ];
        }

        return $group;
    }

    /**
     * 查询格式
     * @return array|array[]
     */
    private function parseQuery(): array
    {
        $filterMust = [];
        $filterMustNot = [];
        $query = [];
        foreach ($this->where as $vWhere) {
            list($column, $operator, $value) = $vWhere;
            switch ($operator) {
                case '=':
                    $filterMust[] = ['term' => [$column => $value]];
                    break;
                case '!=':
                case '<>':
                    $filterMustNot[] = $filterMust[] = ['term' => [$column => $value]];
                    break;
                case '>':
                    $filterMust[] = ['range' => [$column => ['gt' => $value]]];
                    break;
                case '<':
                    $filterMust[] = ['range' => [$column => ['lt' => $value]]];
                    break;
                case '>=':
                    $filterMust[] = ['range' => [$column => ['gte' => $value]]];
                    break;
                case '<=':
                    $filterMust[] = ['range' => [$column => ['lte' => $value]]];
                    break;
                case 'in':
                    $filterMust[] = ['terms' => [$column => $value]];
                    break;
                case 'not in':
                    $filterMustNot[] = ['terms' => [$column => $value]];
                    break;
                case 'like':
                    $filterMust[] = ['wildcard' => [$column => sprintf('*%s*', $value)]];
                    break;
                case 'between':
                    $filterMust[] = ['range' => [$column => ['gte' => $value[0], 'lte' => $value[1]]]];
                    break;
                case 'exists':
                    $filterMust[] = ['exists' => [$column => $value]];

            }
        }

        if (!empty($filterMust)) {
            $query['bool']['filter']['bool']['must'] = $filterMust;
        }

        if (!empty($filterMustNot)) {
            $query['bool']['filter']['bool']['must_not'] = $filterMustNot;
        }

        return $query;
    }

    /**
     * 连接elasticsearch查询方法
     * @param $method
     * @param ...$parameters
     * @return mixed|null
     */
    private function run($method, ...$parameters)
    {
        $client = $this->client;
        $sql = $this->sql;
        if (strpos($method, '.')) {
            $methods = explode('.', $method);
            $method = $methods[1];
            $client = $client->{$methods[0]}();
        }
        dump($sql);
//        Log::get()->info("日志ES-elasticsearch dsl", ['method' => $method, 'sql' => $sql]);
        return call([$client, $method], $parameters);
    }

    /**
     * 设置表
     * @param Model $model
     * @return $this
     */
    public function setModel(Model $model)
    {
        $this->model = $model;
        $this->client = $model->getClient();

        return $this;
    }


    /**
     * 分页
     * @param int $page
     * @param int $size
     * @return Paginator
     */
    public function page(int $page = 1, int $size = 100): Paginator
    {
        $this->page = $page;
        $this->size = $size;
        $this->sqlCombine();
        $result = $this->run('search', $this->sql);
        $collection = $this->formatData($result);


        return make(Paginator::class, ['items' => $collection, 'perPage' => $size, 'currentPage' => $page]);
    }

    /**
     * 查询数据
     * @return Collection
     */
    public function get(): Collection
    {
        $this->sqlCombine();
        $result = $this->run('search', $this->sql);
//        dump($result);
//        exit;
        $collection = $this->formatData($result);
        return $collection;
    }

    private function formatData(array $data): Collection
    {
        if ($this->isAgg == 0) { //非聚合
            $list = $data['hits']['hits'] ?? [];
            $collection = Collection::make($list)->map(function ($value) {
                $attributes = $value['_source'] ?? [];
                if (!empty($attributes)) {
                    $attributes['id'] = $value['_id'] ?? '';
                }
                $model = $this->model->newInstance();
                $model->setAttributes($attributes);
                $model->setOriginal($value);
                return $model;
            });
        } else { //聚合
            if (empty($this->group)) { //没有分组
                $list = $data['aggregations'] ?? [];
                $keys = array_keys($list);
                $attributes = [];
                foreach ($keys as $vKey) {
                    $attributes[$vKey] = $list[$vKey]['value'] ?? '';
                }
                $model = $this->model->newInstance();
                $model->setAttributes($attributes);
                $model->setOriginal($data);

                $collection = Collection::make($model);
            } else { //没有分组
                $list = $data['aggregations']['self_group']['buckets'] ?? [];
                $collection = Collection::make($list)->map(function ($value) {
                    $keys = array_keys($value);
                    $attributes = [];
                    foreach ($keys as $vKey) {
                        if (in_array($vKey, ['key', 'doc_count'])) {
                            $attributes[$vKey] = $value[$vKey] ?? '';
                        } else {
                            $attributes[$vKey] = $value[$vKey]['value'] ?? '';
                        }
                    }
                    $model = $this->model->newInstance();
                    $model->setAttributes($attributes);
                    $model->setOriginal($value);
                    return $model;
                });
            }
        }

        return $collection;
    }
}