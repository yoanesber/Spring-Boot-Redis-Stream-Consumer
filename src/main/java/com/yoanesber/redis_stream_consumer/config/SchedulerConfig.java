package com.yoanesber.redis_stream_consumer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class SchedulerConfig {

    // Konfigurasi task scheduler
    @Value("${spring.task.scheduling.pool.size}")
    private int poolSize;

    @Value("${spring.task.scheduling.thread-name-prefix}")
    private String threadNamePrefix;

    @Value("${spring.task.scheduling.remove-on-cancel-policy}")
    private boolean removeOnCancelPolicy;

    @Value("${spring.task.scheduling.wait-for-tasks-to-complete-on-shutdown}")
    private boolean waitForTasksToCompleteOnShutdown;

    @Value("${spring.task.scheduling.await-termination-seconds}")
    private int awaitTerminationSeconds;

    @Value("${spring.task.scheduling.thread-priority}")
    private int threadPriority;

    @Value("${spring.task.scheduling.continue-existing-periodic-tasks-after-shutdown-policy}")
    private boolean continueExistingPeriodicTasksAfterShutdownPolicy;

    @Value("${spring.task.scheduling.execute-existing-delayed-tasks-after-shutdown-policy}")
    private boolean executeExistingDelayedTasksAfterShutdownPolicy;

    @Value("${spring.task.scheduling.daemon}")
    private boolean daemon;

    /*
    *   ThreadPoolTaskScheduler adalah implementasi dari TaskScheduler yang menggunakan thread pool untuk menjalankan tugas-tugas terjadwal secara asinkron.
    *   Beberapa konfigurasi yang dapat diatur antara lain:
    *   1. poolSize
            - Menentukan jumlah maksimum thread dalam thread pool yang digunakan untuk menangani tugas-tugas terjadwal secara asinkron
            - Jika jumlah tugas lebih besar dari poolSize, tugas yang baru akan menunggu hingga ada thread yang tersedia
            - Jika jumlah thread 0, maka scheduler akan menggunakan jumlah thread yang sama dengan jumlah tugas yang dijadwalkan.
            - Nilai default adalah 1.
    *   2. threadNamePrefix
            - Menentukan prefix nama thread yang digunakan oleh scheduler.
            - Ini sangat berguna untuk membedakan thread yang digunakan oleh scheduler dengan thread yang digunakan oleh aplikasi lain.
            - Nilai default adalah "task-".
    *   3. removeOnCancelPolicy
            - Menentukan apakah scheduler harus menghapus tugas yang dibatalkan dari antrian.
            - Pembatalan tugas adalah proses membatalkan tugas yang sedang berjalan atau tugas yang belum dijalankan.
            - Pembatalan yang dimaksud adalah karena adanya pemanggilan futureTask.cancel(true)
            - Jika terjadi Exception yang Tidak Ditangani (Unhandled Exception) sehingga membunuh semua thread, hal ini tidak berpengaruh dengan fungsi dari setRemoveOnCancelPolicy, karena setRemoveOnCancelPolicy hanya berlaku jika kita memanggil future.cancel(true) secara eksplisit
            - Nilai default adalah false.
    *   4. waitForTasksToCompleteOnShutdown
            - Memastikan bahwa semua tugas yang masih dalam antrian atau sedang berjalan akan diselesaikan sebelum scheduler benar-benar berhenti setelah shutdown() dipanggil
            - Jika true, scheduler akan menunggu semua tugas selesai sebelum aplikasi benar-benar mati.
            - Jika false, scheduler akan mematikan semua tugas yang sedang berjalan tanpa menunggu.
            - Gabungkan dengan setAwaitTerminationSeconds(int seconds) untuk mengatur batas waktu menunggu.
            - Jika setAwaitTerminationSeconds(int seconds) tidak diset, scheduler tidak akan pernah mati(terus menunggu) selama masih ada tugas yang berjalan.
            - Jika ada tugas yang stuck/macet, aplikasi bisa tidak pernah berhenti
            - Jika tugas yang sedang berjalan penting, maka set ke true.
            - Nilai default adalah false.
    *   5. awaitTerminationSeconds
            - Menentukan berapa lama ThreadPoolTaskScheduler akan menunggu sebelum benar-benar menghentikan semua thread setelah dipanggil shutdown()
            - Ketika scheduler.shutdown() dipanggil, scheduler akan Menghentikan penerimaan tugas baru dan menyelesaikan tugas yang sedang berjalan.
            - Jika tugas yang sedang berjalan melebihi waktu ini, scheduler akan mematikan tugas yang sedang berjalan secara paksa.
            - Jika waktu ini 0, scheduler akan mematikan semua tugas yang sedang berjalan tanpa menunggu.
            - Ini penting untuk memastikan semua thread yang sedang berjalan bisa diselesaikan dengan baik sebelum aplikasi benar-benar mati.
            - Nilai default adalah 0.
    *   6. threadPriority
            - Menentukan prioritas eksekusi thread dalam ThreadPoolTaskScheduler
            - Semakin tinggi prioritas thread, semakin cepat thread tersebut dieksekusi oleh sistem dibandingkan thread dengan prioritas lebih rendah
            - Ini sangat berguna jika tugas yang dijadwalkan memiliki prioritas yang berbeda.
            - Gunakan dengan bijak untuk tugas-tugas yang benar-benar perlu dijalankan lebih cepat dibanding yang lain, karena dapat menyebabkan kelaparan (starvation) bagi thread lain yang lebih rendah prioritasnya
            - Nilai default adalah 5.
    *   7. continueExistingPeriodicTasksAfterShutdownPolicy
            - Mengatur apakah tugas periodik (scheduleAtFixedRate atau scheduleWithFixedDelay) akan tetap berjalan setelah shutdown() dipanggil.
            - Jika true, tugas periodik yang sudah dijadwalkan tetap berjalan meskipun shutdown() dipanggil. Tetapi, tidak menerima tugas baru
            - Jika false, semua tugas periodik dihentikan setelah shutdown(). 
            - Nilai default adalah false.
    *   8. executeExistingDelayedTasksAfterShutdownPolicy
            - Menentukan apakah tugas yang tertunda (delayed tasks) akan tetap dieksekusi setelah shutdown() dipanggil.
            - Jika true, tugas yang sudah dijadwalkan (tapi belum berjalan) tetap akan dieksekusi setelah shutdown().
            - Jika false, tugas yang tertunda akan dibatalkan setelah shutdown().
            - Namun jika banyak tugas tertunda yang belum dijalankan, bisa memperlama waktu shutdown.
            - Nilai default adalah true.
    *   9. daemon
            - Menentukan apakah thread yang digunakan oleh scheduler adalah daemon atau bukan.
            - Thread daemon adalah jenis thread yang berjalan di latar belakang dan tidak mencegah JVM berhenti jika semua thread non-daemon telah selesai.
            - Gunakan setDaemon(true) jika tugas latar belakang tidak perlu bertahan setelah aplikasi berakhir.
            - Gunakan setDaemon(false) jika ada tugas penting yang harus selesai sebelum aplikasi dihentikan.
            - Jika daemon diaktifkan (setDaemon(true)) dan semua thread utama selesai, JVM akan berhenti meskipun masih ada task yang tertunda.
            - Jika daemon dinonaktifkan (setDaemon(false)), JVM akan menunggu sampai semua task selesai sebelum benar-benar berhenti.
            - Nilai default adalah false.
    */

    // Scheduler untuk tugas umum
    @Bean(name = "threadPoolTaskScheduler")
    public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();

        // Konfigurasi scheduler
        scheduler.setPoolSize(poolSize);
        scheduler.setThreadNamePrefix(threadNamePrefix);
        scheduler.setRemoveOnCancelPolicy(removeOnCancelPolicy);
        scheduler.setWaitForTasksToCompleteOnShutdown(waitForTasksToCompleteOnShutdown);
        scheduler.setAwaitTerminationSeconds(awaitTerminationSeconds);
        scheduler.setThreadPriority(threadPriority);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(continueExistingPeriodicTasksAfterShutdownPolicy);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(executeExistingDelayedTasksAfterShutdownPolicy);
        scheduler.setDaemon(daemon);
        scheduler.initialize();

        return scheduler;
    }
}