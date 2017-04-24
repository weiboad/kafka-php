<?php

$str = '{"topic":"我的海洋之约","url":"https:\/\/huati.weibo.com\/4659541","hotval":"15827519","thread":{"category1":"美图","category2":"风景风光","title":"","content":"春风拂面，你还不出去浪？阳光、沙滩、海浪、比基尼，春色撩人，你拍了吗>？即日起参与话题#我的海洋之约#，拍摄以“大海”为主题的摄影作品，活动会选出10位幸运网友送出三天两晚免费长隆海洋王国的>奇妙之旅~更多活动“幺蛾子”，敬请期待~","created_at":"Thu Mar 30 10:34:09 +0800 2017","province":"0","city":"0","id":"4659541","is_hot":1,"topic_name":"我的海洋之约"},"hotRank":"0.000000","hot_time":"1492778134","disscnt":15827519,"day_disscnt":365,"mainbody":"我的海洋之约"}';
var_dump(json_decode($str, true));
