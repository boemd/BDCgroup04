clc; close all; clear global; clearvars;
set(0,'defaultTextInterpreter','latex')

k_all = 10:10:200;

y_all_cluster = [10.5483; 9.9642; 9.7861; 9.6639; 9.5462; 9.4961; 9.3317; ...
				9.3204; 9.2578; 9.1879; 9.1743; 9.1061; 9.0946; 9.0689; ...
				9.0431; 9.0115; 8.9946; 8.9458; 8.9466; 8.9113];

y_2M_cluster = [ 10.2092; 9.5775; 9.2798; 9.0856; 8.9951; 8.9671; ...
				8.9588; 8.9301; 8.8520; 8.8300];

figure('Name','Max Diversity Distance on Cluster')
plot(k_all, y_all_cluster, 'LineWidth', 1.5);
grid on; hold on;
plot(10:10:100, y_2M_cluster, 'LineWidth', 1.5);
% title('Max Diversity distance for 5000000 samples (cluster)');
xlabel('k (Number of clusterings)');
xlim([k_all(1) 100]); ylim([8 11]);
legend('vectors-50-all', 'vectors-50-2M');
set(legend,'Interpreter','latex');
hold off;

coreset_t_all = [26519; 25519; 23942; 25159; 23727; 30821; 25678; 27404;...
				37561; 32148; 68265; 73367; 59042; 62462; 48574; 36865; ...
				56385; 52730; 69069; 82165];
			
final_t_all = [24; 40; 70; 155; 297; 449; 726; 1060; 1527; ...
2122; 2818; 3578; 4454; 5745; 8027; 12089; 11927; 12659; 14400; 16747];

figure('Name','Execution Times ALL on Cluster')
plot(k_all, coreset_t_all, 'LineWidth', 1.5);
grid on; hold on;
plot(k_all, final_t_all, 'LineWidth', 1.5);
plot(k_all, coreset_t_all+final_t_all, 'LineWidth', 1.5);
% title('Execution times for 5000000 samples (cluster)');
xlim([k_all(1) 100]); ylim([0 45000]);
xlabel('k (Number of clusterings)'); ylabel('ms');
legend({'Coreset construction', 'Final clustering', 'Total'}, 'Location', 'northwest');
set(legend,'Interpreter','latex');
hold off;

k_2M = 10:10:100;

coreset_t_2M = [12636; 9732; 14897; 12534; 14074; 18627; 23357; 24393; 30550; 22076];

final_t_2M = [43; 103; 109; 172; 484;455; 876; 1141; 1790; 2093];

figure('Name','Execution Times 2M on Cluster')
plot(k_2M, coreset_t_2M, 'LineWidth', 1.5);
grid on; hold on;
plot(k_2M, final_t_2M, 'LineWidth', 1.5);
plot(k_2M, coreset_t_2M+final_t_2M, 'LineWidth', 1.5);
% title('Execution times for 5000000 samples (cluster)');
xlim([k_2M(1) k_2M(end)]); ylim([0 45000]);
xlabel('k (Number of clusterings)'); ylabel('ms');
legend({'Coreset construction', 'Final clustering', 'Total'}, 'Location', 'northwest');
set(legend,'Interpreter','latex');
hold off;