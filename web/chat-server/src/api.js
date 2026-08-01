import axios from "axios";
import store from "./store";

// P0 修复：创建带认证拦截器的 axios 实例
const instance = axios.create({
  baseURL: store.state.backendUrl,
  timeout: 30000,
});

// 请求拦截器：自动添加 Authorization 头
instance.interceptors.request.use(
  (config) => {
    const token = store.state.userInfo.token;
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// 响应拦截器：401 时跳转登录
instance.interceptors.response.use(
  (response) => {
    return response;
  },
  (error) => {
    if (error.response && error.response.status === 401) {
      store.commit("cleanUserInfo");
      window.location.href = "/login";
    }
    return Promise.reject(error);
  }
);

export default instance;
