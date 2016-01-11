(ns dmhy-spider.core
  (:require [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as jdbc]
            [clj-http.client :as client])
  (:gen-class))

(def db
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "database.db"})

(defn init-db [db]
  (jdbc/execute! db ["CREATE TABLE IF NOT EXISTS data (
                      id          INTEGER,
                      name        VARCHAR,
                      time        VARCHAR,
                      magnet      VARCHAR,
                      author      VARCHAR,
                      team        VARCHAR,
                      category    VARCHAR,
                      description VARCHAR,
                      all_size    VARCHAR,
                      file_list   VARCHAR)"])
  (jdbc/execute! db ["CREATE TABLE IF NOT EXISTS last_id (
                      id INTEGER DEFAULT 0)"])
  (jdbc/execute! db ["INSERT INTO last_id SELECT 0
                      WHERE NOT EXISTS (SELECT * FROM last_id)"])
  (jdbc/execute! db ["CREATE TABLE IF NOT EXISTS error (
                      id          INTEGER,
                      code        INTEGER,
                      description VARCHAR)"]))

(defn rand-ip []
  (format "%s.%s.%s.%s"
          (rand-int 256) (rand-int 256)
          (rand-int 256) (rand-int 256)))

(defn gen-topic-url [id]
  (format "https://share.dmhy.org/topics/view/%s_.html" id))

(def reg-find-err #"ui-state-error[a-zA-z0-9<>/\"-=\n\s]*提示信息:[a-zA-z0-9<>/\"-=\n\s]*>(.{0,}?)<")

(defn regsec [re s] (second (re-find re s)))

(defn check-error [resp]
  (if-let [err (regsec reg-find-err (resp :body))]
    err
    (when (not= (resp :status) 200)
      (resp :body))))

(def reg-get-team #"所屬發佈組：[^>]*>([^<>]{1,}?)<")
(def reg-get-author #"發佈人：[^>]*>([^<>]{1,}?)<")
(def reg-get-name #"<h3>([^<>]{1,}?)<\/h3>")
(def reg-get-magnet #"magnet.*href=\"(magnet:?.{1,}?)\"")
(def reg-get-time #"發佈時間: [^>]*>([^<>]{1,}?)<")
(def reg-get-category #"所屬分類: [\w\W]{0,}?<font[^>]*>([^<>]{1,}?)<\/font>")
(def reg-get-all-size #"文件大小: [^>]*>([^<>]{1,}?)<")
(def reg-get-description #"(?:topic-nfo|topic-info)[^>]*>\n*([\W\w]{1,}?)<\/div>\n?.*description-end")
(def reg-get-file-list #"class=\"file_list\">([\w\W]{1,}?)<\/div")
(def reg-get-file-info #"<li>[^>]*>([^<>]{1,}?)\s*<[^>]*>([^<>]{1,}?)<")

(defn parse-file-list [html]
  (when html
    (->> (re-seq reg-get-file-info html)
         (map #(array-map :name (second %) :size (last %)))
         (json/write-str))))

(defn process-page [id]
  (let [url (gen-topic-url id)
        resp (client/get url {:headers {"X-Forwarded-For" (rand-ip)}
                              :throw-exceptions false})
        body (resp :body)
        code (resp :status)]
    (if-let [error (check-error resp)]
      (do (log/warnf "ID: %s HTTP: %s, %s" id code error)
          (jdbc/insert! db "error"
                        {:id id
                         :code code
                         :description error}))
      (let [data {:id id
                  :team (regsec reg-get-team body)
                  :author (regsec reg-get-author body)
                  :name (regsec reg-get-name body)
                  :magnet (regsec reg-get-magnet body)
                  :time (regsec reg-get-time body)
                  :category (regsec reg-get-category body)
                  :all_size (regsec reg-get-all-size body)
                  :description (regsec reg-get-description body)
                  :file_list (->> body
                                  (regsec reg-get-file-list)
                                  (parse-file-list))}]
        (log/infof "ID: %s TITLE: %s" id (data :name))
        (jdbc/insert! db "data" data)))))

(defn parse-int [s]
   (Integer. (re-find  #"\d+" s )))

(defn -main
  ([max-id]
   (init-db db)
   (let [min-id (-> (jdbc/query db ["SELECT id FROM last_id"])
                    (first)
                    (get :id)
                    (+ 1)
                    (str))]
     (-main min-id max-id)))
  ([min-id max-id]
   (init-db db)
   (let [min-id (parse-int min-id)
         max-id (parse-int max-id)]
     (log/infof "%s - %s" min-id max-id)
     (doseq [id (range min-id max-id)]
       (let [t (System/currentTimeMillis)]
         (process-page id)
         (jdbc/update! db "last_id" {:id id} [])
         (when (> (- (System/currentTimeMillis) t) 1000)
           ; 服务器压力过大，歇一会别玩坏
           (log/warn "Process time out, sleep 5 sec")
           (Thread/sleep 5000)))))))

