<?php

declare(strict_types=1);

namespace Liguizhou\Elasticsearch;

use Hyperf\Contract\LengthAwarePaginatorInterface;
use Hyperf\Utils\Collection;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Utils\Context;

/**
 * Class Builder
 * @package Elasticsearch
 * Date : 2023/7/14
 * Author: lgz
 * Desc: ElasticSearch 链式操作
 */
class Builder
{
    use ExcuteBuilder;

    //查询条件符
    const OPERATE = ['=', '>', '<', '>=', '<=', '!=', '<>', 'in', 'not in', 'like', 'between', 'exists', 'not exists'];

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
     * 从哪条开始查起
     * @var int
     */
    protected int $offset = 0;

    /**
     * 查询条数
     * @var int
     */
    protected int $limit = 0;

    /**
     * 总条数
     * @var int
     */
    protected int $count = 0;

    /**
     * 是否为统计条数
     * @var int
     */
    protected int $isCount = 0;

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
            if (is_null($value)) {
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
        $column = is_array($column) ? $column : func_get_args();
        $this->group = $column;
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

    public function select($params): Builder
    {
        $fields = is_array($params) ? $params : func_get_args();
        foreach ($fields as $field) {
            $fieldClass = new Field($field);
            $this->fields[$fieldClass->aliasField] = $fieldClass;
            if ($fieldClass->isAgg != 0) {
                $this->isAgg = 1;
            }
        }
        return $this;
    }

    public function addSelect(...$params): Builder
    {
        return $this->select(...$params);
    }

    public function selectRaw(...$params): Builder
    {
        $fields = is_array($params) ? $params : func_get_args();
        foreach ($fields as $field) {
            $fieldClass = new Field($field);
            $this->fields[$fieldClass->aliasField] = $fieldClass;
            if ($fieldClass->isAgg != 0) {
                $this->isAgg = 1;
            }
        }
        return $this;
    }

    public function addSelectRaw(...$params): Builder
    {
        return $this->selectRaw(...$params);
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

        if ($this->isCount == 0) {
            if ($this->isAgg == 0) {
                $this->normalSqlCombine();
            } else {
                $this->aggSqlCombile();
            }
        } else {
            $this->countSqlCombile();
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
        if ($this->limit != 0) { //等于0查所有数据
            $this->sql['body']['size'] = $this->limit;
            $this->sql['body']['from'] = $this->offset;
        }
//        if ($this->isCount == 1) { //计算条数一定不查询详细数据
//            $this->sql['body']['size'] = 0;
//        }
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
                if ($vField->aggType == 'raw') { //原生查询
                    $fieldArr[$vField->aliasField] = $vField->field;
                } else {
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
            if ($this->limit != 0) {
                $aggQuery['aggs']['self_sort']['bucket_sort']['size'] = $this->limit;
                $aggQuery['aggs']['self_sort']['bucket_sort']['from'] = $this->offset;
            }
        }

        if (empty($group)) {
            $this->sql['body']['aggs'] = $aggQuery['aggs'];
        } else {
            $this->sql['body']['aggs']['self_group'] = $aggQuery;
        }

        $this->sql['body']['size'] = 0; //聚合不需要详细数据

    }

    /**
     * 查询条数拼接
     * @return void
     */
    private function countSqlCombile()
    {
//
//        'my_count' => [
//        'cardinality' => [
//            'script' => [
//                'lang' => 'painless',
//                'source' => "doc['pid'].value + '_' + doc['put_date'].value"
//            ]
//        ]
//    ]

        if ($this->isAgg == 0) {
            $this->sql['body']['size'] = 0;
        } else {
            $group = $this->parseGroup();
            $this->sql['body']['aggs']['self_count']['cardinality'] = $group;

            $this->sql['body']['size'] = 0; //不需要详细数据
        }
    }

    /**
     * 分组格式
     * @return array|array[]
     */
    private function parseGroup(): array
    {
        $group = [];
        $groupString = '';
        $ifGroupString = '';
        foreach ($this->group as $vGroup) {
            $ifGroupString .= "doc['{$vGroup}'].size() > 0 &&";
            $groupString .= "doc['{$vGroup}'].value + '_' +";
        }
        $groupString = trim($groupString, '+');
        $ifGroupString = trim($ifGroupString, '&');
        $groupString = "if (".$ifGroupString.") { " . $groupString . " } else { '' }" ;

        if (!empty($this->group)) {
            $group = [
                'script' => [
                    "lang"   => "painless",
                    'source' => $groupString
                ],
            ];
            if ($this->isCount == 0) {
                $group['size'] = 1000000; //排序分页会根据父聚合的个数进行的，所以这里暂时设置为一百万
                $group['shard_size'] = 1000000;
            }
        } else { //没有分组的情况下给一个桶
            $group = [
                'script' => [
                    "lang"   => "painless",
                    'source' => "1"
                ],
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
                    $filterMustNot[] = ['term' => [$column => $value]];
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
                    $filterMust[] = ['terms' => [$column => array_values($value)]];
                    break;
                case 'not in':
                    $filterMustNot[] = ['terms' => [$column => array_values($value)]];
                    break;
                case 'like':
                    $filterMust[] = ['wildcard' => [$column => sprintf('*%s*', $value)]];
                    break;
                case 'between':
                    $filterMust[] = ['range' => [$column => ['gte' => $value[0], 'lte' => $value[1]]]];
                    break;
                case 'exists':
                    $filterMust[] = ['exists' => ['field' => $column]];
                    break;
                case 'not exists':
                    $filterMustNot[] = ['exists' => ['field' => $column]];
                    break;
            }
        }

        //如果文档中值不存在，然后用这个分组查询，会报错的，所以要加上条件排除不存在的值
        if (!empty($this->group)) {
            foreach ($this->group as $value) {
//                $filterMust[] = ['exists' => ['field' => $value]];
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
     * @return mixed|null
     */
    private function run($method)
    {
        $client = $this->client;
        $sql = $this->sql;
        if (strpos($method, '.')) {
            $methods = explode('.', $method);
            $method = $methods[1];
            $client = $client->{$methods[0]}();
        }
        if ($this->model->getDebug()) {
            dump($sql);
        }
        if ($this->model->getLog()) {
            ApplicationContext::getContainer()
                ->get(LoggerFactory::class)
                ->get('log', 'elasticsearch')
                ->info('elasticsearch_sql:', ['method' => $method, 'sql' => json_encode($sql)]);
            ApplicationContext::getContainer()
                ->get(LoggerFactory::class)
                ->get('log', 'elasticsearch')
                ->info('elasticsearch_param:', ['order' => $this->order, 'group' => $this->group, 'where' => $this->where, 'fields' => $this->fields, 'offset' => $this->offset, 'limit' => $this->limit, 'isAgg' => $this->isAgg, 'isCount' => $this->isCount]);
        }
        try {
            $result = call([$client, $method], [$sql]);

            $took = $result['took'] ?? 0;
            if ($this->model->getDebug()) {
                dump('耗时：' . $took / 1000 . '秒');
            }
            if ($took > 3000) { //超过3秒定义为慢查询
                ApplicationContext::getContainer()
                    ->get(LoggerFactory::class)
                    ->get('log', 'elasticsearch')
                    ->info('elasticsearch_slow_sql:', ['method' => $method, 'sql' => json_encode($sql)]);
            }
        } catch (\Exception $e) {
            if ($this->model->getDebug()) {
                dump($e->getMessage());
            }
            if ($this->model->getLog()) {
                ApplicationContext::getContainer()
                    ->get(LoggerFactory::class)
                    ->get('log', 'elasticsearch')
                    ->error('elasticsearch_error:', ['msg' => $e->getMessage()]);
                throw new \Exception($e->getMessage());
            }
        }

        return $result;
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

    public function offset(int $offset): Builder
    {
        $this->offset = $offset;
        return $this;
    }

    public function limit(int $limit): Builder
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * 分页
     * @param int $currentPage
     * @param int $perPage
     * @return LengthAwarePaginatorInterface
     */
    public function paginate(): LengthAwarePaginatorInterface
    {
        $currentPage = intval(floor($this->offset / $this->limit) + 1);
        $perPage = $this->limit;

        $this->sqlCombine();
        $result = $this->run('search');

        $items = $this->formatData($result);
        $options = $this->sql;

        //查询总条数
        $total = $this->count();

        $container = ApplicationContext::getContainer();
        return $container->make(LengthAwarePaginatorInterface::class, compact('items', 'total', 'perPage', 'currentPage', 'options'));
    }

    /**
     * 查询数据
     * @return Collection
     */
    public function get(): Collection
    {
        $this->sqlCombine();
        $result = $this->run('search');
        $collection = $this->formatData($result);
        return $collection;
    }

    public function chunk($limit, callable $func)
    {
        $this->limit = $limit;
        $i = 0;
        while (true) {
            $this->offset = $limit * $i;
            $result = $this->get();
            if (count($result) <= 0) {
                break;
            }

            $func($result);
            if ($i >= 1000) { //最多只能1000次分页，防止数据量过大
                break;
            }
            $i++;
        }
    }

    public function first(): ?Model
    {
        $this->limit(1);
        $this->sqlCombine();
        $result = $this->run('search');
        $collection = $this->formatData($result);

        return $collection[0] ?? $this->model->newInstance();
    }

    public function count()
    {
        $this->isCount = 1;
        if ($this->isAgg == 1 && empty($this->parseGroup())) { //聚合查询但是没有分组的条数为1
            $this->count = 1;
        } else {
            $this->order = []; //算总数没有排序
            $this->fields = []; //算总数没有字段
            $this->sqlCombine();
            if ($this->isAgg == 0) {
                $this->sql['body']['track_total_hits'] = true; //文档数量大于10000时，需要加上
            }

            $result = $this->run('search');
            if ($this->isAgg == 0) {
                $this->count = (int)$result['hits']['total']['value'] ?? 0;
            } else {
                $this->count = (int)$result['aggregations']['self_count']['value'] ?? 0;
            }
        }

        return $this->count;
    }

    public function find($id)
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'id'    => $id
        ];
        $result = $this->run('get');
        $data = $result['_source'] ?? [];
        $data['id'] = $result['_id'] ?? '';

        $model = $this->model->newInstance();
        $model->setAttributes($data, array_keys($this->fields));
        $model->setOriginal($result);
        return $model;
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
                $model->setAttributes($attributes, array_keys($this->fields));
                $model->setOriginal($value);
                return $model;
            });
        } else { //聚合
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
                $model->setAttributes($attributes, array_keys($this->fields));
                $model->setOriginal($value);
                return $model;
            });
        }

        return $collection;
    }

    public function getIndexConfig()
    {
        $body = [
            'index' => $this->model->getIndex()
        ];

        $this->sql = $body;

        return $this->run('indices.getMapping');
    }

    public function getIndex()
    {
        return $this->model->getIndex();
    }

    public function setIndex($index)
    {
        $this->model->setIndex($index);
        return $this;
    }
}
